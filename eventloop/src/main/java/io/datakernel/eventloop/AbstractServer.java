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
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;

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

	private InetSocketAddress[] listenAddresses;
	private ServerSocketChannel[] serverSocketChannels;
	protected InetAddressRange range;

	// JMX
	private static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private double smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
	private final EventStats totalAccepts = new EventStats(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats rangeBlocked = new EventStats();
	private final ExceptionStats prepareSocketException = new ExceptionStats();
	private final ExceptionStats closeException = new ExceptionStats();

	// creators & builder methods
	public AbstractServer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	@SuppressWarnings("unchecked")
	protected S self() {
		return (S) this;
	}

	public S acceptedIpAddresses(InetAddressRange range) {
		this.range = range;
		return self();
	}

	public S serverSocketSettings(ServerSocketSettings serverSocketSettings) {
		this.serverSocketSettings = checkNotNull(serverSocketSettings);
		return self();
	}

	public S socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return self();
	}

	public S setListenAddresses(InetSocketAddress... listenAddresses) {
		this.listenAddresses = checkNotNull(listenAddresses);
		return self();
	}

	public S setListenAddresses(List<InetSocketAddress> addresses) {
		setListenAddresses(addresses.toArray(new InetSocketAddress[addresses.size()]));
		return self();
	}

	public S setListenAddress(InetSocketAddress address) {
		setListenAddresses(address);
		return self();
	}

	public S setListenPort(int port) {
		setListenAddress(new InetSocketAddress(port));
		return self();
	}

	/**
	 * Sets the flag as true, which means that this server can handle only one accepting.
	 */
	public S acceptOnce() {
		this.acceptOnce = true;
		return self();
	}

	/**
	 * Sets the flag which means possible accepting to this server. If it is true, this server can
	 * accept only one socketChannel, else - as much as you need.
	 */
	public S acceptOnce(boolean acceptOnce) {
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
		if (listenAddresses != null) {
			serverSocketChannels = new ServerSocketChannel[listenAddresses.length];
			for (int i = 0; i < listenAddresses.length; i++) {
				try {
					serverSocketChannels[i] = eventloop.listen(listenAddresses[i], serverSocketSettings, this);
				} catch (IOException exception) {
					logger.error("Can't listen on {}", this, listenAddresses[i]);
					close();
					throw exception;
				}
			}
			logger.info("Listening on {}", Arrays.toString(listenAddresses));
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

	public CompletionCallbackFuture listenFuture() {
		return AsyncCallbacks.listenFuture(this);
	}

	public CompletionCallbackFuture closeFuture() {
		return AsyncCallbacks.closeFuture(this);
	}

	protected boolean isRunning() {
		return running;
	}

	private void closeServerSocketChannels() {
		if (serverSocketChannels == null || serverSocketChannels.length == 0) {
			return;
		}
		for (int i = 0; i < serverSocketChannels.length; i++) {
			if (serverSocketChannels[i] == null) {
				continue;
			}
			closeQuietly(serverSocketChannels[i]);
			serverSocketChannels[i] = null;
		}
	}

	private void closeQuietly(@Nullable AutoCloseable closeable) {
		if (closeable == null)
			return;
		try {
			closeable.close();
		} catch (Exception e) {
			// jmx
			closeException.recordException(e, closeable);

			if (logger.isWarnEnabled()) {
				logger.warn("Exception thrown while closing {}", closeable, e);
			}
		}
	}

	// core
	@Override
	public final void onAccept(SocketChannel socketChannel) {
		assert eventloop.inEventloopThread();
		try {
			InetAddress remoteAddress = ((InetSocketAddress) socketChannel.getRemoteAddress()).getAddress();
			if (isInRange(remoteAddress) && checkRemoteAddress(remoteAddress)) {
				doAccept(socketChannel);
			} else {
				rangeBlocked.recordEvent();
				resolveRangeBlock(socketChannel);
			}
		} catch (IOException e) {
			if (logger.isWarnEnabled()) {
				logger.warn("Exception thrown while trying to get remoteAddress from socket {}", socketChannel, e);
			}
		}
	}

	protected void doAccept(SocketChannel socketChannel) {
		totalAccepts.recordEvent();
		prepareSocket(socketChannel);
		SocketConnection connection = createConnection(socketChannel);
		connection.register();
		if (acceptOnce) {
			close();
		}
	}

	protected void prepareSocket(SocketChannel socketChannel) {
		try {
			socketSettings.applySettings(socketChannel);
		} catch (IOException e) {
			prepareSocketException.recordException(e, socketChannel);
			if (logger.isErrorEnabled()) {
				logger.error("Exception thrown while apply settings socket {}", socketChannel, e);
			}
		}
	}

	protected boolean checkRemoteAddress(InetAddress address) {
		return true;
	}

	private boolean isInRange(InetAddress address) {
		return range == null || range.contains(address);
	}

	protected void resolveRangeBlock(SocketChannel socketChannel) {
		try {
			socketChannel.close();
		} catch (IOException e) {
			if (logger.isWarnEnabled()) {
				logger.warn("Exception thrown on socket close {} while trying to resolve range block", socketChannel, e);
			}
		}
	}

	protected abstract SocketConnection createConnection(SocketChannel socketChannel);

	// jmx
	@JmxAttribute
	public ExceptionStats getCloseException() {
		return closeException;
	}

	@JmxAttribute
	public ExceptionStats getPrepareSocketException() {
		return prepareSocketException;
	}

	@JmxAttribute
	public EventStats getTotalAccepts() {
		return totalAccepts;
	}

	@JmxAttribute
	public EventStats getRangeBlocked() {
		return rangeBlocked;
	}

	@JmxAttribute
	public final double getSmoothingWindow() {
		return smoothingWindow;
	}

	@JmxAttribute
	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;

		totalAccepts.setSmoothingWindow(smoothingWindow);
	}
}
