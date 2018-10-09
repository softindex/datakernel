/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.eventloop;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.net.CloseWithoutNotifyException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import static io.datakernel.bytebuf.ByteBufPool.recycleIfEmpty;
import static io.datakernel.util.Recyclable.tryRecycle;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;

public final class AsyncSslSocket implements AsyncTcpSocket {
	private final SSLEngine engine;
	private final Executor executor;
	private final AsyncTcpSocket upstream;

	private ByteBuf net2engine = ByteBuf.empty();
	private ByteBuf engine2app = ByteBuf.empty();
	private ByteBuf app2engine = ByteBuf.empty();

	@Nullable
	private SettableStage<ByteBuf> read;
	@Nullable
	private SettableStage<Void> write;

	// region builders
	public static AsyncSslSocket wrapClientSocket(AsyncTcpSocket asyncTcpSocket,
			String host, int port,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
		sslEngine.setUseClientMode(true);
		return create(asyncTcpSocket, sslEngine, executor);
	}

	public static AsyncSslSocket wrapClientSocket(AsyncTcpSocket asyncTcpSocket,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(true);
		return create(asyncTcpSocket, sslEngine, executor);
	}

	public static AsyncSslSocket wrapServerSocket(AsyncTcpSocket asyncTcpSocket,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(false);
		return create(asyncTcpSocket, sslEngine, executor);
	}

	private AsyncSslSocket(AsyncTcpSocket asyncTcpSocket, SSLEngine engine, Executor executor) {
		this.engine = engine;
		this.executor = executor;
		this.upstream = asyncTcpSocket;
		startHandShake();
	}

	public static AsyncSslSocket create(AsyncTcpSocket asyncTcpSocket,
			SSLEngine engine, Executor executor) {
		return new AsyncSslSocket(asyncTcpSocket, engine, executor);
	}
	// endregion

	private <T> Stage<T> sanitize(Stage<T> stage) {
		return stage.thenComposeEx((value, e) -> {
			if (e == null) {
				return Stage.of(value);
			} else {
				close(e);
				return Stage.ofException(e);
			}
		});
	}

	@Override
	public Stage<ByteBuf> read() {
		if (!isOpen()) return Stage.ofException(CLOSE_EXCEPTION);
		this.read = null;
		if (engine2app.canRead()) {
			ByteBuf readBuf = engine2app;
			engine2app = ByteBuf.empty();
			return Stage.of(readBuf);
		}
		SettableStage<ByteBuf> read = new SettableStage<>();
		this.read = read;
		sync();
		return read;
	}

	@Override
	public Stage<Void> write(@Nullable ByteBuf buf) {
		if (!isOpen()) {
			if (buf != null) {
				buf.recycle();
			}
			return Stage.ofException(CLOSE_EXCEPTION);
		}
		if (buf == null) {
			throw new UnsupportedOperationException("SSL cannot work in half-duplex mode");
		}
		app2engine = ByteBufPool.append(app2engine, buf);
		if (this.write != null) return write;
		SettableStage<Void> write = new SettableStage<>();
		this.write = write;
		sync();
		return write;
	}

	private void doRead() {
		sanitize(upstream.read())
				.whenResult(buf -> {
					assert isOpen();
					if (buf != null) {
						net2engine = ByteBufPool.append(net2engine, buf);
						sync();
					} else {
						if (engine.isInboundDone()) return;
						try {
							engine.closeInbound();
						} catch (SSLException e) {
							close(new CloseWithoutNotifyException(AsyncSslSocket.class, "Peer closed without sending close_notify", e));
						}
					}
				});
	}

	private void doWrite(ByteBuf dstBuf) {
		sanitize(upstream.write(dstBuf))
				.whenResult($ -> {
					assert isOpen();
					if (engine.isOutboundDone()) {
						close();
						return;
					}
					if (!app2engine.canRead() && engine.getHandshakeStatus() == NOT_HANDSHAKING && write != null) {
						SettableStage<Void> write = this.write;
						this.write = null;
						write.set(null);
					}
				});
	}

	private SSLEngineResult tryToUnwrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = net2engine.toReadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toWriteByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.unwrap(srcBuffer, dstBuffer);
		} catch (SSLException e) {
			dstBuf.recycle();
			throw e;
		} catch (RuntimeException e) {
			// https://bugs.openjdk.java.net/browse/JDK-8072452
			dstBuf.recycle();
			throw new SSLException(e);
		}

		net2engine.ofReadByteBuffer(srcBuffer);
		net2engine = recycleIfEmpty(net2engine);

		dstBuf.ofWriteByteBuffer(dstBuffer);
		if (isOpen() && dstBuf.canRead()) {
			engine2app = ByteBufPool.append(engine2app, dstBuf);
		} else {
			dstBuf.recycle();
		}

		return result;
	}

	private SSLEngineResult tryToWrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = app2engine.toReadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toWriteByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.wrap(srcBuffer, dstBuffer);
		} catch (SSLException e) {
			dstBuf.recycle();
			throw e;
		} catch (RuntimeException e) {
			// https://bugs.openjdk.java.net/browse/JDK-8072452
			dstBuf.recycle();
			throw new SSLException(e);
		}

		app2engine.ofReadByteBuffer(srcBuffer);
		app2engine = recycleIfEmpty(app2engine);

		dstBuf.ofWriteByteBuffer(dstBuffer);
		if (dstBuf.canRead()) {
			doWrite(dstBuf);
		} else {
			dstBuf.recycle();
		}
		return result;
	}

	/**
	 * This method is used for handling handshake routine as well as sending close_notify message to recepient
	 */
	private void doHandshake() throws SSLException {
		SSLEngineResult result = null;
		while (true) {
			if (result != null && result.getStatus() == CLOSED) {
				close();
				return;
			}

			HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == NEED_WRAP) {
				result = tryToWrap();
			} else if (handshakeStatus == NEED_UNWRAP) {
				result = tryToUnwrap();
				if (result.getStatus() == BUFFER_UNDERFLOW) {
					doRead();
					return;
				}
			} else if (handshakeStatus == NEED_TASK) {
				executeTasks();
				return;
			} else {
				doSync();
				return;
			}
		}
	}

	private void executeTasks() {
		while (true) {
			Runnable task = engine.getDelegatedTask();
			if (task == null) break;
			Stage.ofRunnable(executor, task)
					.whenResult($ -> {
						if (!isOpen()) return;
						try {
							doHandshake();
						} catch (SSLException e) {
							close(e);
						}
					});
		}
	}

	private void sync() {
		try {
			doSync();
		} catch (SSLException e) {
			close(e);
		}
	}

	private void doSync() throws SSLException {
		if (!isOpen()) return;
		SSLEngineResult result = null;
		HandshakeStatus handshakeStatus = engine.getHandshakeStatus();

		if (handshakeStatus != NOT_HANDSHAKING) {
			doHandshake();
			return;
		}

		// write data to net
		if (app2engine.canRead()) {
			do {
				result = tryToWrap();
			}
			while (isOpen() && app2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
		}

		if (!isOpen()) {
			return;
		}

		// read data from net
		if (net2engine.canRead()) {
			do {
				result = tryToUnwrap();
			} while (net2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));

			if (read != null && engine2app.canRead()) {
				SettableStage<ByteBuf> read = this.read;
				this.read = null;
				ByteBuf readBuf = engine2app;
				engine2app = ByteBuf.empty();
				read.set(readBuf);
				return;
			}
		}

		if (result != null && isOpen() && result.getStatus() == CLOSED) {
			close();
			return;
		}

		doRead();
	}

	private void startHandShake() {
		try {
			engine.beginHandshake();
			sync();
		} catch (SSLException e) {
			close(e);
		}
	}

	private boolean isOpen() {
		return net2engine != null;
	}

	@SuppressWarnings("AssignmentToNull") // bufs set to null only when socket is closing
	private void recycleByteBufs() {
		tryRecycle(net2engine);
		tryRecycle(engine2app);
		tryRecycle(app2engine);
		net2engine = engine2app = app2engine = null;
	}

	@Override
	public void close(Throwable e) {
		if (!isOpen()) return;
		if (!engine.isOutboundDone()) {
			engine.closeOutbound();
			sync(); // sync is used here to send close_notify message to recepient (will be sent once)
		}
		recycleByteBufs();
		upstream.close(e);
		if (write != null) {
			write.setException(e);
			write = null;
		}
		if (read != null) {
			read.setException(e);
			read = null;
		}
	}
}
