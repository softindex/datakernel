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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;

public final class AsyncSslSocket implements AsyncTcpSocket, AsyncTcpSocket.EventHandler {
	private static final Logger logger = LoggerFactory.getLogger(AsyncSslSocket.class);

	private final Eventloop eventloop;
	private final SSLEngine engine;
	private final Executor executor;
	private final AsyncTcpSocket upstream;

	private AsyncTcpSocket.EventHandler downstreamEventHandler;

	private ByteBuf net2engine = ByteBuf.empty();
	private ByteBuf app2engine = ByteBuf.empty();

	private boolean syncPosted = false;
	private boolean flushAndClose = false;

	private static AsyncSslSocket wrapSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLContext sslContext, Executor executor, boolean clientMode) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(clientMode);
		AsyncSslSocket asyncSslSocket = new AsyncSslSocket(eventloop, asyncTcpSocket, sslEngine, executor);
		asyncTcpSocket.setEventHandler(asyncSslSocket);
		return asyncSslSocket;
	}

	public static AsyncSslSocket wrapClientSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLContext sslContext, Executor sslExecutor) {
		return wrapSocket(eventloop, asyncTcpSocket, sslContext, sslExecutor, true);
	}

	public static AsyncSslSocket wrapServerSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLContext sslContext, Executor executor) {
		return wrapSocket(eventloop, asyncTcpSocket, sslContext, executor, false);
	}

	public AsyncSslSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLEngine engine, Executor executor) {
		this.eventloop = eventloop;
		this.engine = engine;
		this.executor = executor;
		this.upstream = asyncTcpSocket;
	}

	@Override
	public void onRegistered() {
		downstreamEventHandler.onRegistered();
		try {
			engine.beginHandshake();
			doSync();
		} catch (SSLException e) {
			handleSSLException(e, true);
		}
	}

	@Override
	public void onRead(ByteBuf buf) {
		net2engine = ByteBufPool.append(net2engine, buf);
		sync();
	}

	@Override
	public void onShutdownInput() {
		if (engine.isInboundDone()) return;
		try {
			engine.closeInbound();
		} catch (SSLException e) {
			downstreamEventHandler.onClosedWithError(e);
//			engine.closeOutbound();  // try to send close_notify
			sync();
		}
	}

	@Override
	public void onWrite() {
		if (engine.isOutboundDone()) {
			upstream.close();
			return;
		}
		if (!app2engine.canRead() && engine.getHandshakeStatus() == NOT_HANDSHAKING) {
			downstreamEventHandler.onWrite();
		}
	}

	@Override
	public void onClosedWithError(Exception e) {
		downstreamEventHandler.onClosedWithError(e);
	}

	@Override
	public void setEventHandler(EventHandler eventHandler) {
		this.downstreamEventHandler = eventHandler;
	}

	@Override
	public void read() {
		upstream.read();
		postSync();
	}

	private void postSync() {
		if (!syncPosted) {
			syncPosted = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					sync();
				}
			});
		}
	}

	@Override
	public void write(ByteBuf buf) {
		assert !flushAndClose;

		app2engine = ByteBufPool.append(app2engine, buf);
		postSync();
	}

	@Override
	public void shutdownOutput() {
		throw new UnsupportedOperationException("SSL cannot work in half-duplex mode");
	}

	@Override
	public void close() {
		engine.closeOutbound();
		postSync();
	}

	@Override
	public void flushAndClose() {
		flushAndClose = true;
		postSync();
	}

	@Override
	public InetSocketAddress getRemoteSocketAddress() {
		return upstream.getRemoteSocketAddress();
	}

	private void handleSSLException(final SSLException e, boolean post) {
		upstream.close();
		if (post) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					downstreamEventHandler.onClosedWithError(e);
				}
			});
		} else {
			downstreamEventHandler.onClosedWithError(e);
		}
	}

	private SSLEngineResult tryToUnwrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = net2engine.toHeadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toTailByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.unwrap(srcBuffer, dstBuffer);
			logger.trace("" + result);
		} catch (SSLException e) {
			net2engine.recycle();
			dstBuf.recycle();
			throw e;
		}

		net2engine.ofHeadByteBuffer(srcBuffer);
		if (!net2engine.canRead()) {
			net2engine.recycle();
			net2engine = ByteBuf.empty();
		}

		dstBuf.ofTailByteBuffer(dstBuffer);
		if (dstBuf.canRead()) {
			downstreamEventHandler.onRead(dstBuf);
		} else {
			dstBuf.recycle();
		}

		return result;
	}

	private SSLEngineResult tryToWrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = app2engine.toHeadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toTailByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.wrap(srcBuffer, dstBuffer);
			logger.trace("" + result);
		} catch (SSLException e) {
			dstBuf.recycle();
			throw e;
		}

		app2engine.ofHeadByteBuffer(srcBuffer);
		if (!app2engine.canRead()) {
			app2engine.recycle();
			app2engine = ByteBuf.empty();
		}

		dstBuf.ofTailByteBuffer(dstBuffer);
		if (dstBuf.canRead()) {
			upstream.write(dstBuf);
		} else {
			dstBuf.recycle();
		}
		return result;
	}

	private void executeTasks() {
		while (true) {
			final Runnable task = engine.getDelegatedTask();
			if (task == null) break;
			executor.execute(new Runnable() {
				@Override
				public void run() {
					task.run();
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							sync();
						}
					});
				}
			});
		}
	}

	private void sync() {
		syncPosted = false;
		try {
			doSync();
		} catch (SSLException e) {
			handleSSLException(e, false);
		}
	}

	@SuppressWarnings("UnusedAssignment")
	private void doSync() throws SSLException {
		HandshakeStatus handshakeStatus;
		SSLEngineResult result = null;
		while (true) {
			handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == NEED_WRAP) {
				result = tryToWrap();
				if (result.getHandshakeStatus() == FINISHED) {
					upstream.read();
				}
			} else if (handshakeStatus == NEED_UNWRAP) {
				result = tryToUnwrap();
				if (result.getStatus() == BUFFER_UNDERFLOW) {
					upstream.read();
					break;
				}
				if (result.getHandshakeStatus() == FINISHED) {
					upstream.read();
				}
			} else if (handshakeStatus == NEED_TASK) {
				executeTasks();
				return;
			} else if (handshakeStatus == NOT_HANDSHAKING) {

				// read data from net
				if (net2engine.canRead()) {
					do {
						result = tryToUnwrap();
					} while (net2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
					if (result.getStatus() == BUFFER_UNDERFLOW) {
						upstream.read();
					}
					if (result.getStatus() == CLOSED) {
						downstreamEventHandler.onShutdownInput();
					}
				}

				// write data to net
				if (app2engine.canRead()) {
					do {
						result = tryToWrap();
					} while (app2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
				}
				if (!app2engine.canRead() && flushAndClose) {
					engine.closeOutbound();
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}

}