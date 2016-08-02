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
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallbackFuture;
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
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.AsyncSslSocket.wrapServerSocket;
import static io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * It is implementation of {@link EventloopServer}. It is non-blocking server which works in eventloop.
 * The server runs on the one thread, and all events are fired on that thread. This server can listen few
 * addresses in one time and you can register multiple connections for responding to incoming data.
 *
 * @param <S> type of AbstractNioServer which extends from it
 */
@SuppressWarnings("WeakerAccess, unused")
public abstract class AbstractServer<S extends AbstractServer<S>> implements EventloopServer, EventloopJmxMBean {
	private static final Logger logger = getLogger(AbstractServer.class);

	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = new ServerSocketSettings(DEFAULT_BACKLOG);

	private ServerSocketSettings serverSocketSettings = DEFAULT_SERVER_SOCKET_SETTINGS;
	private SocketSettings socketSettings = defaultSocketSettings();

	protected final Eventloop eventloop;

	private boolean running = false;
	protected boolean acceptOnce;

	private List<InetSocketAddress> listenAddresses;
	private List<ServerSocketChannel> serverSocketChannels;

	protected InetAddressRange range;
	private Set<InetAddress> bannedAddresses;

	// ssl
	private SSLContext sslContext;
	private ExecutorService sslExecutor;
	private List<InetSocketAddress> sslListenAddresses;

	// JMX
	private static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private double smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
	private final EventStats accepts = new EventStats(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats rangeBlocked = new EventStats(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats bannedBlocked = new EventStats(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats notAllowed = new EventStats(DEFAULT_SMOOTHING_WINDOW);

	// creators & builder methods
	public AbstractServer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	@SuppressWarnings("unchecked")
	protected S self() {
		return (S) this;
	}

	public final S acceptedIpAddresses(InetAddressRange range) {
		this.range = range;
		return self();
	}

	public final S serverSocketSettings(ServerSocketSettings serverSocketSettings) {
		this.serverSocketSettings = checkNotNull(serverSocketSettings);
		return self();
	}

	public final S socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return self();
	}

	public final S setListenAddresses(List<InetSocketAddress> addresses) {
		ensureNotIntersect(sslListenAddresses, addresses);
		this.listenAddresses = checkNotNull(addresses);
		return self();
	}

	public final S setListenAddresses(InetSocketAddress... addresses) {
		return setListenAddresses(Arrays.asList(addresses));
	}

	public final S setListenAddress(InetSocketAddress address) {
		return setListenAddresses(address);
	}

	public final S setListenPort(int port) {
		return setListenAddress(new InetSocketAddress(port));
	}

	public final S setSslListenAddresses(SSLContext sslContext, ExecutorService executor, List<InetSocketAddress> addresses) {
		ensureNotIntersect(listenAddresses, addresses);
		this.sslContext = checkNotNull(sslContext);
		this.sslExecutor = checkNotNull(executor);
		this.sslListenAddresses = checkNotNull(addresses);
		return self();
	}

	public final S setSslListenAddresses(SSLContext sslContext, ExecutorService executor, InetSocketAddress... addresses) {
		return setSslListenAddresses(sslContext, executor, Arrays.asList(addresses));
	}

	public final S setSslListenAddress(SSLContext sslContext, ExecutorService executor, InetSocketAddress address) {
		return this.setSslListenAddresses(sslContext, executor, Collections.singletonList(address));
	}

	public final S setSslListenPort(SSLContext sslContext, ExecutorService executor, int port) {
		return setSslListenAddress(sslContext, executor, new InetSocketAddress(port));
	}

	public final S banAddresses(Collection<InetAddress> addresses) {
		bannedAddresses = new HashSet<>(checkNotNull(addresses));
		return self();
	}

	public final S banAddresses(InetAddress... addresses) {
		return banAddresses(Arrays.asList(addresses));
	}

	public final S banAddress(InetAddress address) {
		return banAddresses(Collections.singleton(address));
	}

	/**
	 * Sets the flag as true, which means that this server can handle only one accepting.
	 */
	public final S acceptOnce() {
		return acceptOnce(true);
	}

	/**
	 * Sets the flag which means possible accepting to this server. If it is true, this server can
	 * accept only one socketChannel, else - as much as you need.
	 */
	public final S acceptOnce(boolean acceptOnce) {
		this.acceptOnce = acceptOnce;
		return self();
	}

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
		if (listenAddresses != null) {
			listenAddresses(listenAddresses, false);
			logger.info("Listening on {}", listenAddresses);
		}
		if (sslListenAddresses != null) {
			listenAddresses(sslListenAddresses, true);
			logger.info("Listening SSL on {}", sslListenAddresses);
		}
	}

	private void listenAddresses(List<InetSocketAddress> addresses, final boolean ssl) throws IOException {
		for (InetSocketAddress address : addresses) {
			try {
				serverSocketChannels.add(eventloop.listen(address, serverSocketSettings, new AcceptCallback() {
					@Override
					public void onAccept(SocketChannel socketChannel) {
						AbstractServer.this.onAccept(socketChannel, ssl);
					}
				}));
			} catch (IOException e) {
				logger.error("Can't listen on {}", this, address);
				close();
				throw e;
			}
		}
	}

	@Override
	public final void close() {
		check(eventloop.inEventloopThread());
		if (!running)
			return;
		running = false;
		closeServerSocketChannels();
		onClose();
	}

	protected void onListen() {
	}

	protected void onClose() {
	}

	public final CompletionCallbackFuture listenFuture() {
		return AsyncCallbacks.listenFuture(this);
	}

	public final CompletionCallbackFuture closeFuture() {
		return AsyncCallbacks.closeFuture(this);
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

	private boolean satisfiesRestrictions(InetAddress remoteAddress) {
		if (!(isInRange(remoteAddress))) {
			rangeBlocked.recordEvent();
			return false;
		}
		if (isBanned(remoteAddress)) {
			bannedBlocked.recordEvent();
			return false;
		}
		if (!canAccept(remoteAddress)) {
			notAllowed.recordEvent();
			return false;
		}
		return true;
	}

	private boolean isInRange(InetAddress address) {
		return range == null || range.contains(address);
	}

	private boolean isBanned(InetAddress remoteAddress) {
		return bannedAddresses != null && bannedAddresses.contains(remoteAddress);
	}

	protected boolean canAccept(InetAddress address) {
		return true;
	}

	protected EventloopServer getWorkerServer() {
		return this;
	}

	private void onAccept(final SocketChannel socketChannel, final boolean ssl) {
		assert eventloop.inEventloopThread();

		InetAddress remoteAddress = null;
		try {
			remoteAddress = ((InetSocketAddress) socketChannel.getRemoteAddress()).getAddress();
		} catch (IOException e) {
		}

		if (satisfiesRestrictions(remoteAddress)) {
			accepts.recordEvent();
			final EventloopServer workerServer = getWorkerServer();
			Eventloop workerServerEventloop = workerServer.getEventloop();

			final AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(workerServerEventloop, socketChannel, socketSettings);
			final AsyncTcpSocket asyncTcpSocket = ssl ? wrapServerSocket(workerServerEventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;

			if (workerServerEventloop == this.eventloop) {
				workerServer.onAccept(asyncTcpSocket);
				asyncTcpSocketImpl.register();
			} else {
				workerServerEventloop.execute(new Runnable() {
					@Override
					public void run() {
						workerServer.onAccept(asyncTcpSocket);
						asyncTcpSocketImpl.register();
					}
				});
			}

			if (acceptOnce) {
				close();
			}
		} else {
			closeQuietly(socketChannel);
		}
	}

	protected abstract EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket);

	@Override
	public final void onAccept(AsyncTcpSocket asyncTcpSocket) {
		assert eventloop.inEventloopThread();
		asyncTcpSocket.setEventHandler(createSocketHandler(asyncTcpSocket));
	}

	private boolean isInetAddressAny(InetSocketAddress listenAddress) {
		return listenAddress.getAddress().isAnyLocalAddress();
	}

	private void ensureNotIntersect(List<InetSocketAddress> container, List<InetSocketAddress> addresses) {
		if (container != null) {
			for (InetSocketAddress address : addresses) {
				check(!container.contains(address), "{} already listened to", address);
			}
		}
	}

	// jmx
	@JmxAttribute(description = "successful accepts")
	public final EventStats getAccepts() {
		return accepts;
	}

	@JmxAttribute(description = "rejected attempts to connect: remote address was not in specified range")
	public final EventStats getRangeBlocked() {
		return rangeBlocked;
	}

	@JmxAttribute(description = "rejected attempts to connect: remote address was banned")
	public final EventStats getBannedBlocked() {
		return bannedBlocked;
	}

	@JmxAttribute(description = "rejected attempts to connect: remote address was not allowed by any other reason")
	public final EventStats getNotAllowed() {
		return notAllowed;
	}

	@JmxAttribute(description = "sum of all rejected attempts to connect (rangeBlocked, bannedBlocked, notAllowed)")
	public final long getTotalBlocked() {
		return rangeBlocked.getTotalCount() + bannedBlocked.getTotalCount() + notAllowed.getTotalCount();
	}
}
