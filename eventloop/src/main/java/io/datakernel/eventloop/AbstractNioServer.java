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
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.ExceptionMarker;
import org.slf4j.Logger;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * It is implementation of {@link NioServer}. It is non-blocking server which works in eventloop.
 * The server runs on the one thread, and all events are fired on that thread. This server can listen few
 * addresses in one time and you can register multiple connections for responding to incoming data.
 *
 * @param <S> type of AbstractNioServer which extends from it
 */
public abstract class AbstractNioServer<S extends AbstractNioServer<S>> implements NioServer, AbstractNioServerMBean {
	private static final Logger logger = getLogger(AbstractNioServer.class);

	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = new ServerSocketSettings(DEFAULT_BACKLOG);
	/**
	 * Creates a new default socket settings for creating new sockets with its values.
	 */
	private ServerSocketSettings serverSocketSettings = DEFAULT_SERVER_SOCKET_SETTINGS;
	private SocketSettings socketSettings = defaultSocketSettings();

	protected final NioEventloop eventloop;

	private boolean running = false;
	protected boolean acceptOnce;
	private InetSocketAddress[] listenAddresses;
	private ServerSocketChannel[] serverSocketChannels;

	// JMX
	private static final ExceptionMarker PREPARE_SOCKET_MARKER = new ExceptionMarker(PrimaryNioServer.class, "PrepareSocketException");
	private static final ExceptionMarker CLOSE_MARKER = new ExceptionMarker(PrimaryNioServer.class, "CloseException");
	protected long totalAccepts;

	public AbstractNioServer(NioEventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	@SuppressWarnings("unchecked")
	protected S self() {
		return (S) this;
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

	/**
	 * Begins to listen all addresses which was added to this server.
	 *
	 * @throws IOException If some  I/O error occurs
	 */
	@Override
	public final void listen() throws IOException {
		check(eventloop.inEventloopThread());
		if (running)
			return;
		running = true;
		onListen();
		if (listenAddresses != null) {
			serverSocketChannels = new ServerSocketChannel[listenAddresses.length];
			try {
				for (int i = 0; i < listenAddresses.length; i++) {
					serverSocketChannels[i] = eventloop.listen(listenAddresses[i], serverSocketSettings, this);
				}
			} catch (IOException exception) {
				close();
				throw exception;
			}
		}
	}

	protected void onListen() {
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

	public CompletionCallbackFuture listenFuture() {
		return AsyncCallbacks.listenFuture(this);
	}

	public CompletionCallbackFuture closeFuture() {
		return AsyncCallbacks.closeFuture(this);
	}

	protected void onClose() {
	}

	/**
	 * Closes all server socket channels for safe ending of acceptor's work. Releases resources
	 * that the object holds.
	 */
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
			eventloop.updateExceptionCounter(CLOSE_MARKER, e, closeable);
			if (logger.isWarnEnabled()) {
				logger.warn(CLOSE_MARKER.getMarker(), "Exception thrown while closing {}", closeable, e);
			}
		}
	}

	/**
	 * Returns eventloop which is related with this NioServer
	 */
	@Override
	public final NioEventloop getNioEventloop() {
		return eventloop;
	}

	protected void prepareSocket(SocketChannel socketChannel) {
		try {
			socketSettings.applySettings(socketChannel);
		} catch (IOException e) {
			eventloop.updateExceptionCounter(PREPARE_SOCKET_MARKER, e, socketChannel);
			if (logger.isErrorEnabled()) {
				logger.error(PREPARE_SOCKET_MARKER.getMarker(), "Exception thrown while apply settings socket {}", socketChannel, e);
			}
		}
	}

	/**
	 * Accepts incoming socket connections without blocking event loop thread. Creates a new connection
	 * and registers it in eventloop.
	 *
	 * @param socketChannel the incoming socketChannel.
	 */
	@Override
	public void onAccept(SocketChannel socketChannel) {
		assert eventloop.inEventloopThread();
		totalAccepts++;
		prepareSocket(socketChannel);
		SocketConnection connection = createConnection(socketChannel);
		connection.register();
		if (acceptOnce) {
			close();
		}
	}

	/**
	 * Returns connection from this socketChannel. You should override it to create connection
	 * with your logic.
	 *
	 * @param socketChannel the socketChannel for creating connection.
	 * @return new SocketConnection.
	 */
	protected abstract SocketConnection createConnection(SocketChannel socketChannel);

	// JMX
	@Override
	public void resetStats() {
		totalAccepts = 0;
		eventloop.resetExceptionCounter(PREPARE_SOCKET_MARKER);
		eventloop.resetExceptionCounter(CLOSE_MARKER);
	}

	@Override
	public long getTotalAccepts() {
		return totalAccepts;
	}

	@Override
	public CompositeData getLastPrepareSocketException() throws OpenDataException {
		LastExceptionCounter exceptionCounter = eventloop.getExceptionCounter(PREPARE_SOCKET_MARKER);
		return (exceptionCounter == null) ? null : exceptionCounter.compositeData();
	}

	@Override
	public CompositeData getLastCloseException() throws OpenDataException {
		LastExceptionCounter exceptionCounter = eventloop.getExceptionCounter(CLOSE_MARKER);
		return (exceptionCounter == null) ? null : exceptionCounter.compositeData();
	}

}
