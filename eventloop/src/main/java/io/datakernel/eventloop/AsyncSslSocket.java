/*
 * Copyright (C) 2015 SoftIndex LLC.
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
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;

@SuppressWarnings("AssertWithSideEffects")
public final class AsyncSslSocket implements AsyncTcpSocket {
	private final Eventloop eventloop;
	private final SSLEngine engine;
	private final Executor executor;
	private final AsyncTcpSocket upstream;

	private ByteBuf net2engine = ByteBuf.empty();
	private ByteBuf engine2app = ByteBuf.empty();
	private ByteBuf app2engine = ByteBuf.empty();

	private boolean syncPosted = false;
	private SettableStage<ByteBuf> read;
	private SettableStage<Void> write;

	// region builders
	public static AsyncSslSocket wrapClientSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
			String host, int port,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
		sslEngine.setUseClientMode(true);
		return create(eventloop, asyncTcpSocket, sslEngine, executor);
	}

	public static AsyncSslSocket wrapClientSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(true);
		return create(eventloop, asyncTcpSocket, sslEngine, executor);
	}

	public static AsyncSslSocket wrapServerSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(false);
		return create(eventloop, asyncTcpSocket, sslEngine, executor);
	}

	private AsyncSslSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLEngine engine, Executor executor) {
		this.eventloop = eventloop;
		this.engine = engine;
		this.executor = executor;
		this.upstream = asyncTcpSocket;
	}

	public static AsyncSslSocket create(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
			SSLEngine engine, Executor executor) {
		return new AsyncSslSocket(eventloop, asyncTcpSocket, engine, executor);
	}
	// endregion

	private void start() {
		try {
			engine.beginHandshake();
			doSync();
		} catch (SSLException e) {
			upstream.close();
			eventloop.post(() -> {
				if (engine2app != null) {
					closeWithError(e);
				}
				recycleByteBufs();
			});
		}
	}

	private <T> Stage<T> sanitize(Stage<T> stage) {
		return stage.async().thenComposeEx((value, e) -> {
			if (e == null) {
				return Stage.of(value);
			} else {
				if (engine2app != null) {
					closeWithError(e);
				}
				recycleByteBufs();
				return Stage.ofException(e);
			}
		});
	}

	@Override
	public void closeWithError(Throwable e) {
		upstream.closeWithError(e);
	}

	@Override
	public Stage<ByteBuf> read() {
		if (read != null) return read;
		read = new SettableStage<>();
		if (!net2engine.canRead() && !engine2app.canRead()) {
			doRead();
		}
		postSync();
		return read;
	}

	private void doRead() {
		sanitize(upstream.read())
				.whenResult(buf -> {
					if (buf != null) {
						net2engine = ByteBufPool.append(net2engine, buf);
						sync();
					} else {
						if (engine.isInboundDone()) return;
						try {
							engine.closeInbound();
						} catch (SSLException e) {
							if (app2engine != null) {
								closeWithError(new CloseWithoutNotifyException(e));
							}
							upstream.close();
							recycleByteBufs();
						}
					}
				});
	}

	@Override
	public Stage<Void> write(@Nullable ByteBuf buf) {
		if (buf == null) {
			throw new UnsupportedOperationException("SSL cannot work in half-duplex mode");
		}
		app2engine = ByteBufPool.append(app2engine, buf);
		if (write != null) return write;
		write = new SettableStage<>();
		postSync();
		return write;
	}

	@Override
	public void close() {
		if (engine2app == null) return;
		engine2app.recycle();
		engine2app = null;
		app2engine.recycle();
		app2engine = ByteBuf.empty();
		engine.closeOutbound();
		postSync();
	}

	private void recycleByteBufs() {
		if (net2engine != null) net2engine.recycle();
		if (engine2app != null) engine2app.recycle();
		if (app2engine != null) app2engine.recycle();
		net2engine = engine2app = app2engine = null;
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
		if (engine2app != null && dstBuf.canRead()) {
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

	private void doWrite(ByteBuf dstBuf) {
		sanitize(upstream.write(dstBuf))
				.thenRun(() -> {
					if (engine.isOutboundDone()) {
						upstream.close();
						recycleByteBufs();
						return;
					}
					if (engine2app != null && !app2engine.canRead() && engine.getHandshakeStatus() == NOT_HANDSHAKING && write != null) {
						SettableStage<Void> write = this.write;
						this.write = null;
						write.set(null);
					}
				});
	}

	private void executeTasks() {
		while (true) {
			Runnable task = engine.getDelegatedTask();
			if (task == null) break;
			executor.execute(() -> {
				task.run();
				eventloop.execute(() -> {
					if (net2engine == null) return;
					sync();
				});
			});
		}
	}

	private void postSync() {
		if (!syncPosted) {
			syncPosted = true;
			eventloop.post(this::sync);
		}
	}

	private void sync() {
		syncPosted = false;
		try {
			doSync();
		} catch (SSLException e) {
			upstream.close();
			if (engine2app != null) {
				closeWithError(e);
			}
			recycleByteBufs();
		}
	}

	@SuppressWarnings("UnusedAssignment")
	private void doSync() throws SSLException {
		HandshakeStatus handshakeStatus;
		SSLEngineResult result = null;
		while (true) {
			if (result != null && result.getStatus() == CLOSED) {
				upstream.close();
				recycleByteBufs();
				break;
			}
			handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == NEED_WRAP) {
				result = tryToWrap();
			} else if (handshakeStatus == NEED_UNWRAP) {
				result = tryToUnwrap();
				if (result.getStatus() == BUFFER_UNDERFLOW) {
					doRead();
					break;
				}
			} else if (handshakeStatus == NEED_TASK) {
				executeTasks();
				return;
			} else if (handshakeStatus == NOT_HANDSHAKING) {
				if (result != null && result.getHandshakeStatus() == FINISHED) {
					doRead();
				}

				// read data from net
				if (net2engine.canRead()) {
					do {
						result = tryToUnwrap();
					} while (net2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
					if (result.getStatus() == BUFFER_UNDERFLOW) {
						doRead();
					}
				}

				// write data to net
				if (app2engine.canRead()) {
					do {
						result = tryToWrap();
					} while (app2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
				}
				break;
			} else
				break;
		}

		if (engine2app != null && read != null && engine2app.canRead()) {
			SettableStage<ByteBuf> read = this.read;
			this.read = null;
			ByteBuf readBuf = engine2app;
			engine2app = ByteBuf.empty();
			read.set(readBuf);
		}

		if (engine2app != null && result != null && result.getStatus() == CLOSED) {
			SettableStage<ByteBuf> read = this.read;
			this.read = null;
			read.set(null);
		}
	}

}
