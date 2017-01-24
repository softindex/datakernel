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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.datakernel.eventloop.AsyncSslSocket.wrapServerSocket;
import static io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.util.Preconditions.check;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * It is implementation of {@link EventloopServer}. It is non-blocking server which works in eventloop.
 * The server runs on the one thread, and all events are fired on that thread. This server can listen few
 * addresses in one time and you can register multiple connections for responding to incoming data.
 *
 * @param <S> type of AbstractNioServer which extends from it
 */
@SuppressWarnings("WeakerAccess, unused")
public abstract class AbstractServer<S extends AbstractServer<S>> implements EventloopServer, WorkerServer, EventloopJmxMBean {
	protected Logger logger = getLogger(this.getClass());

	protected final Eventloop eventloop;

	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = ServerSocketSettings.create(DEFAULT_BACKLOG);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create();

	protected ServerSocketSettings serverSocketSettings = DEFAULT_SERVER_SOCKET_SETTINGS;
	protected SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	private boolean acceptOnce;

	public interface AcceptFilter {
		boolean canAccept(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl);
	}

	protected AcceptFilter acceptFilter;

	private List<InetSocketAddress> listenAddresses = new ArrayList<>();

	// ssl
	private SSLContext sslContext;
	private ExecutorService sslExecutor;
	private List<InetSocketAddress> sslListenAddresses = new ArrayList<>();

	private boolean running = false;
	private List<ServerSocketChannel> serverSocketChannels;

	protected Inspector inspector = new JmxInspector();

	public interface Inspector {
		AsyncTcpSocketImpl.Inspector getSocketInspector(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl);

		void onAccept(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl);

		void onReject(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl);
	}

	public static class JmxInspector implements Inspector {
		private final AsyncTcpSocketImpl.JmxInspector socketStats = new AsyncTcpSocketImpl.JmxInspector();
		private final AsyncTcpSocketImpl.JmxInspector socketStatsSsl = new AsyncTcpSocketImpl.JmxInspector();
		private final EventStats accepts = EventStats.create();
		private final EventStats acceptsSsl = EventStats.create();
		private final EventStats rejects = EventStats.create();

		@Override
		public AsyncTcpSocketImpl.Inspector getSocketInspector(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl) {
			return ssl ? socketStatsSsl : socketStats;
		}

		@Override
		public void onAccept(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl) {
			accepts.recordEvent();
			if (ssl) {
				acceptsSsl.recordEvent();
			}
		}

		@Override
		public void onReject(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl) {
			rejects.recordEvent();
		}

		@JmxAttribute(description = "successful accepts")
		public final EventStats getAccepts() {
			return accepts;
		}

		@JmxAttribute
		public EventStats getAcceptsSsl() {
			return acceptsSsl;
		}

		@JmxAttribute
		public EventStats getRejects() {
			return rejects;
		}

		@JmxAttribute
		public AsyncTcpSocketImpl.JmxInspector getSocketStats() {
			return socketStats;
		}

		@JmxAttribute
		public AsyncTcpSocketImpl.JmxInspector getSocketStatsSsl() {
			return socketStatsSsl;
		}
	}

	// region creators & builder methods
	protected AbstractServer(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@SuppressWarnings("unchecked")
	protected S self() {
		return (S) this;
	}

	public final S withAcceptFilter(AcceptFilter acceptFilter) {
		this.acceptFilter = acceptFilter;
		return self();
	}

	public final S withServerSocketSettings(ServerSocketSettings serverSocketSettings) {
		this.serverSocketSettings = serverSocketSettings;
		return self();
	}

	public final S withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return self();
	}

	public final S withListenAddresses(List<InetSocketAddress> addresses) {
		this.listenAddresses = addresses;
		return self();
	}

	public final S withListenAddresses(InetSocketAddress... addresses) {
		return withListenAddresses(asList(addresses));
	}

	public final S withListenAddress(InetSocketAddress address) {
		return withListenAddresses(singletonList(address));
	}

	public final S withListenPort(int port) {
		return withListenAddress(new InetSocketAddress(port));
	}

	public final S withSslListenAddresses(SSLContext sslContext, ExecutorService sslExecutor, List<InetSocketAddress> addresses) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		this.sslListenAddresses = addresses;
		return self();
	}

	public final S withSslListenAddresses(SSLContext sslContext, ExecutorService sslExecutor, InetSocketAddress... addresses) {
		return withSslListenAddresses(sslContext, sslExecutor, asList(addresses));
	}

	public final S withSslListenAddress(SSLContext sslContext, ExecutorService sslExecutor, InetSocketAddress address) {
		return withSslListenAddresses(sslContext, sslExecutor, singletonList(address));
	}

	public final S withSslListenPort(SSLContext sslContext, ExecutorService sslExecutor, int port) {
		return withSslListenAddress(sslContext, sslExecutor, new InetSocketAddress(port));
	}

	public final S withAcceptOnce() {
		return withAcceptOnce(true);
	}

	public final S withAcceptOnce(boolean acceptOnce) {
		this.acceptOnce = acceptOnce;
		return self();
	}

	public final S withLogger(Logger logger) {
		this.logger = logger;
		return self();
	}
	// endregion

	// eventloop server api
	@Override
	public final Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public final void listen() throws IOException {
		check(eventloop.inEventloopThread());
		if (running)
			return;
		running = true;
		onListen();
		serverSocketChannels = new ArrayList<>();
		if (listenAddresses != null && !listenAddresses.isEmpty()) {
			listenAddresses(listenAddresses, false);
			logger.info("Listening on {}", listenAddresses);
		}
		if (sslListenAddresses != null && !sslListenAddresses.isEmpty()) {
			listenAddresses(sslListenAddresses, true);
			logger.info("Listening SSL on {}", sslListenAddresses);
		}
	}

	private void listenAddresses(List<InetSocketAddress> addresses, final boolean ssl) throws IOException {
		for (final InetSocketAddress address : addresses) {
			try {
				ServerSocketChannel serverSocketChannel = eventloop.listen(address, serverSocketSettings, new AcceptCallback() {
					@Override
					public void onAccept(SocketChannel socketChannel) {
						AbstractServer.this.doAccept(socketChannel, address, ssl);
					}
				});
				serverSocketChannels.add(serverSocketChannel);
			} catch (IOException e) {
				logger.error("Can't listen on {}", this, address);
				close(IgnoreCompletionCallback.create());
				throw e;
			}
		}
	}

	protected void onListen() {
	}

	@Override
	public final void close(CompletionCallback callback) {
		check(eventloop.inEventloopThread());
		if (!running)
			return;
		running = false;
		closeServerSocketChannels();
		onClose(callback);
	}

	public final Future<?> closeFuture() {
		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				close(new CompletionCallback() {
					@Override
					protected void onComplete() {
						future.setComplete();
					}

					@Override
					protected void onException(Exception e) {
						future.setException(e);
					}
				});
			}
		});
		return future;
	}

	protected void onClose(final CompletionCallback completionCallback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				completionCallback.setComplete();
			}
		});
	}

	protected boolean isRunning() {
		return running;
	}

	private void closeServerSocketChannels() {
		if (serverSocketChannels == null || serverSocketChannels.isEmpty()) {
			return;
		}
		for (Iterator<ServerSocketChannel> it = serverSocketChannels.iterator(); it.hasNext(); ) {
			ServerSocketChannel serverSocketChannel = it.next();
			if (serverSocketChannel == null) {
				continue;
			}
			closeQuietly(serverSocketChannel);
			it.remove();
		}
	}

	private void closeQuietly(@Nullable AutoCloseable closeable) {
		if (closeable == null)
			return;
		try {
			closeable.close();
		} catch (Exception e) {
			eventloop.recordIoError(e, closeable);
		}
	}

	protected WorkerServer getWorkerServer() {
		return this;
	}

	private void doAccept(final SocketChannel socketChannel, final InetSocketAddress localAddress, final boolean ssl) {
		assert eventloop.inEventloopThread();

		final InetAddress remoteAddress;
		try {
			remoteAddress = ((InetSocketAddress) socketChannel.getRemoteAddress()).getAddress();
		} catch (IOException e) {
			closeQuietly(socketChannel);
			return;
		}

		if (acceptFilter != null && !acceptFilter.canAccept(remoteAddress, localAddress, ssl)) {
			if (inspector != null) inspector.onReject(remoteAddress, localAddress, ssl);
			closeQuietly(socketChannel);
			return;
		}

		final WorkerServer workerServer = getWorkerServer();
		final Eventloop workerServerEventloop = workerServer.getEventloop();

		if (workerServerEventloop == this.eventloop) {
			doAccept(socketChannel, remoteAddress, localAddress, socketSettings, ssl);
		} else {
			if (inspector != null) inspector.onAccept(remoteAddress, localAddress, ssl);
			workerServerEventloop.execute(new Runnable() {
				@Override
				public void run() {
					workerServer.doAccept(socketChannel, remoteAddress, localAddress, socketSettings, ssl);
				}
			});
		}

		if (acceptOnce) {
			close(IgnoreCompletionCallback.create());
		}
	}

	@Override
	public void doAccept(SocketChannel socketChannel, InetAddress remoteAddress, InetSocketAddress localAddress,
	                     SocketSettings socketSettings, final boolean ssl) {
		assert eventloop.inEventloopThread();
		if (inspector != null) inspector.onAccept(remoteAddress, localAddress, ssl);
		final AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings)
				.withInspector(inspector == null ? null : inspector.getSocketInspector(remoteAddress, localAddress, ssl));
		final AsyncTcpSocket asyncTcpSocket = ssl ? wrapServerSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
		asyncTcpSocket.setEventHandler(createSocketHandler(asyncTcpSocket));
		asyncTcpSocketImpl.register();
	}

	protected abstract EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket);

	private boolean isInetAddressAny(InetSocketAddress listenAddress) {
		return listenAddress.getAddress().isAnyLocalAddress();
	}

}





