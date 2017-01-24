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

package io.datakernel.http;

import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ConnectCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.IAsyncDnsClient;
import io.datakernel.eventloop.*;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public final class AsyncHttpClient implements IAsyncHttpClient, EventloopService, EventloopJmxMBean {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create();
	public static final long DEFAULT_KEEP_ALIVE_MILLIS = 30 * 1000L;

	private static final long CHECK_PERIOD = 1000L;

	private final Eventloop eventloop;
	private IAsyncDnsClient asyncDnsClient;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private final ExposedLinkedList<AbstractHttpConnection> keepAlivePool = ExposedLinkedList.create();
	private final HashMap<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> keepAlivePoolsByAddresses = new HashMap<>();
	private final char[] headerChars = new char[MAX_HEADER_LINE_SIZE];

	private boolean closed;
	private AsyncCancellable expiredConnectionsCheck;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	// timeouts
	private int connectTimeoutMillis = 0;
	private long keepAliveTimeMillis = DEFAULT_KEEP_ALIVE_MILLIS;

	// SSL
	private SSLContext sslContext;
	private ExecutorService sslExecutor;

	protected Inspector inspector = new JmxInspector();

	public interface Inspector {
		AsyncTcpSocketImpl.Inspector socketInspector(HttpRequest httpRequest, InetSocketAddress address, boolean https);

		void onRequest(HttpRequest request);

		void onResolve(HttpRequest request, InetAddress[] inetAddresses);

		void onResolveError(HttpRequest request, Exception e);

		void onConnect(HttpRequest request, HttpClientConnection connection);

		void onConnectError(HttpRequest request, InetSocketAddress address, Exception e);

		void onConnectionResponse(HttpClientConnection connection, HttpResponse response);

		void onConnectionException(HttpClientConnection connection, boolean activeConnection, Exception e);

		void onConnectionClosed(HttpClientConnection connection);
	}

	public static class JmxInspector implements Inspector {
		protected final AsyncTcpSocketImpl.JmxInspector socketStats = new AsyncTcpSocketImpl.JmxInspector();
		protected final AsyncTcpSocketImpl.JmxInspector socketStatsForSSL = new AsyncTcpSocketImpl.JmxInspector();
		private final EventStats totalRequests = EventStats.create();
		private final EventStats httpsRequests = EventStats.create();
		private final ExceptionStats resolveErrors = ExceptionStats.create();
		private final EventStats connected = EventStats.create();
		private final ExceptionStats connectErrors = ExceptionStats.create();
		private long responses;
		private final ExceptionStats errorsActive = ExceptionStats.create();
		private final ExceptionStats errorsKeepAlive = ExceptionStats.create();
		private final EventStats closed = EventStats.create();

		@Override
		public AsyncTcpSocketImpl.Inspector socketInspector(HttpRequest httpRequest, InetSocketAddress address, boolean https) {
			return https ? socketStatsForSSL : socketStats;
		}

		@Override
		public void onRequest(HttpRequest request) {
			totalRequests.recordEvent();
			if (request.isHttps())
				httpsRequests.recordEvent();
		}

		@Override
		public void onResolve(HttpRequest request, InetAddress[] inetAddresses) {
		}

		@Override
		public void onResolveError(HttpRequest request, Exception e) {
			resolveErrors.recordException(e, request);
		}

		@Override
		public void onConnect(HttpRequest request, HttpClientConnection connection) {
			connected.recordEvent();
		}

		@Override
		public void onConnectError(HttpRequest request, InetSocketAddress address, Exception e) {
			connectErrors.recordException(e, request);
		}

		@Override
		public void onConnectionResponse(HttpClientConnection connection, HttpResponse response) {
			responses++;
		}

		@Override
		public void onConnectionException(HttpClientConnection connection, boolean activeConnection, Exception e) {
			if (activeConnection)
				errorsActive.recordException(e, "url: " + connection.remoteAddress);
			else
				errorsKeepAlive.recordException(e, "url: " + connection.remoteAddress);
		}

		@Override
		public void onConnectionClosed(HttpClientConnection connection) {
			closed.recordEvent();
		}

		@JmxAttribute
		public AsyncTcpSocketImpl.JmxInspector getSocketStats() {
			return socketStats;
		}

		@JmxAttribute(description = "all requests that were sent (both successful and failed)")
		public EventStats getTotalRequests() {
			return totalRequests;
		}

		@JmxAttribute(description = "successful requests that were sent over secured connection (https)")
		public EventStats getHttpsRequests() {
			return httpsRequests;
		}

		@JmxAttribute
		public ExceptionStats getResolveErrors() {
			return resolveErrors;
		}

		@JmxAttribute(description = "number of \"open connection\" events)")
		public EventStats getConnected() {
			return connected;
		}

		@JmxAttribute(description = "number of \"close connection\" events)")
		public EventStats getClosed() {
			return closed;
		}

		@JmxAttribute
		public ExceptionStats getErrorsActive() {
			return errorsActive;
		}

		@JmxAttribute
		public ExceptionStats getErrorsKeepAlive() {
			return errorsKeepAlive;
		}

		@JmxAttribute(description = "current number of live connections (totally in pool and in use)", reducer = JmxReducers.JmxReducerSum.class)
		public long getActiveConnections() {
			return connected.getTotalCount() - closed.getTotalCount();
		}

		@JmxAttribute
		public long getActiveRequests() {
			return totalRequests.getTotalCount() -
					(resolveErrors.getTotal() + connectErrors.getTotal() + errorsActive.getTotal() + responses);
		}

	}

	private int inetAddressIdx = 0;

	private AsyncHttpClient(Eventloop eventloop, IAsyncDnsClient asyncDnsClient) {
		this.eventloop = eventloop;
		this.asyncDnsClient = asyncDnsClient;
	}

	public static AsyncHttpClient create(Eventloop eventloop) {
		IAsyncDnsClient defaultDnsClient = AsyncDnsClient.create(eventloop);
		return new AsyncHttpClient(eventloop, defaultDnsClient);
	}

	public AsyncHttpClient withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	public AsyncHttpClient withDnsClient(IAsyncDnsClient asyncDnsClient) {
		this.asyncDnsClient = asyncDnsClient;
		return this;
	}

	public AsyncHttpClient withSslEnabled(SSLContext sslContext, ExecutorService sslExecutor) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		return this;
	}

	public AsyncHttpClient withKeepAliveTimeMillis(long keepAliveTimeMillis) {
		this.keepAliveTimeMillis = keepAliveTimeMillis;
		return this;
	}

	public AsyncHttpClient withNoKeepAlive() {
		return withKeepAliveTimeMillis(0);
	}

	public AsyncHttpClient withConnectTimeout(long connectTimeoutMillis) {
		this.connectTimeoutMillis = (int) connectTimeoutMillis;
		return this;
	}

	public AsyncHttpClient withMaxHttpMessageSize(int maxHttpMessageSize) {
		this.maxHttpMessageSize = maxHttpMessageSize;
		return this;
	}

	public AsyncHttpClient withMaxHttpMessageSize(MemSize maxHttpMessageSize) {
		return withMaxHttpMessageSize((int) maxHttpMessageSize.get());
	}

	public AsyncHttpClient withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.scheduleBackground(eventloop.currentTimeMillis() + CHECK_PERIOD, new ScheduledRunnable() {
			@Override
			public void run() {
				expiredConnectionsCheck = null;
				checkExpiredConnections();
				if (!keepAlivePool.isEmpty()) {
					scheduleExpiredConnectionsCheck();
				}
			}
		});
	}

	private int checkExpiredConnections() {
		int count = 0;
		final long now = eventloop.currentTimeMillis();

		ExposedLinkedList.Node<AbstractHttpConnection> node = keepAlivePool.getFirstNode();
		while (node != null) {
			HttpClientConnection connection = (HttpClientConnection) node.getValue();
			node = node.getNext();

			long idleTime = now - connection.keepAliveTimestamp;
			if (idleTime < keepAliveTimeMillis)
				break; // connections must back ordered by activity
			connection.close();
			count++;
		}
		return count;
	}

	private HttpClientConnection takeConnection(InetSocketAddress address) {
		ExposedLinkedList<HttpClientConnection> addressPool = keepAlivePoolsByAddresses.get(address);
		if (addressPool == null)
			return null;
		HttpClientConnection connection = addressPool.getFirstValue();
		if (connection == null)
			return null;
		removeFromPool(connection);
		return connection;
	}

	void returnToPool(final HttpClientConnection connection) {
		assert !connection.isClosed();
		assert !connection.isInPool();
		if (closed || keepAliveTimeMillis == 0) {
			eventloop.execute(new Runnable() {
				@Override
				public void run() {
					connection.close();
				}
			});
			return;
		}

		ExposedLinkedList<HttpClientConnection> addressPool = connection.keepAlivePoolByAddress;
		if (addressPool == null) {
			addressPool = keepAlivePoolsByAddresses.get(connection.remoteAddress);
			if (addressPool == null) {
				addressPool = ExposedLinkedList.create();
				keepAlivePoolsByAddresses.put(connection.remoteAddress, addressPool);
			}
			connection.keepAlivePoolByAddress = addressPool;
		}
		addressPool.addLastNode(connection.keepAlivePoolByAddressNode);
		keepAlivePool.addLastNode(connection.poolNode);
		connection.keepAliveTimestamp = eventloop.currentTimeMillis();

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	void removeFromPool(HttpClientConnection connection) {
		assert connection.isInPool();
		keepAlivePool.removeNode(connection.poolNode);
		connection.keepAlivePoolByAddress.removeNode(connection.keepAlivePoolByAddressNode);
		if (connection.keepAlivePoolByAddress.isEmpty()) {
			keepAlivePoolsByAddresses.remove(connection.remoteAddress);
		}
		connection.keepAlivePoolByAddress = null;
		connection.keepAliveTimestamp = 0L;
	}

	/**
	 * Sends the request to server, waits the result timeout and handles result with callback
	 *
	 * @param request  request for server
	 * @param callback callback for handling result
	 */
	@Override
	public void send(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
		assert eventloop.inEventloopThread();

		if (inspector != null) inspector.onRequest(request);
		getUrlAsync(request, callback);
	}

	private void getUrlAsync(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
		String host = request.getUrl().getHost();
		asyncDnsClient.resolve4(host, new ResultCallback<InetAddress[]>() {
			@Override
			public void onResult(InetAddress[] inetAddresses) {
				if (inspector != null) inspector.onResolve(request, inetAddresses);
				getUrlForHostAsync(request, inetAddresses, callback);
			}

			@Override
			protected void onException(Exception e) {
				if (inspector != null) inspector.onResolveError(request, e);
				callback.setException(e);
			}
		});
	}

	private InetAddress getNextInetAddress(InetAddress[] inetAddresses) {
		return inetAddresses[((inetAddressIdx++) & Integer.MAX_VALUE) % inetAddresses.length];
	}

	private void getUrlForHostAsync(final HttpRequest request, final InetAddress[] inetAddresses, final ResultCallback<HttpResponse> callback) {
		String host = request.getUrl().getHost();
		final InetAddress inetAddress = getNextInetAddress(inetAddresses);
		if (!isValidHost(host, inetAddress)) {
			callback.setException(new IOException("Invalid IP address " + inetAddress + " for host " + host));
			return;
		}

		final InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		HttpClientConnection connection = takeConnection(address);
		if (connection != null) {
			connection.send(request, callback);
			return;
		}

		eventloop.connect(address, connectTimeoutMillis, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				boolean https = request.isHttps();
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings)
						.withInspector(inspector == null ? null : inspector.socketInspector(request, address, https));
				AsyncTcpSocket asyncTcpSocket = https ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;

				HttpClientConnection connection = HttpClientConnection.create(eventloop, address,
						asyncTcpSocket,
						AsyncHttpClient.this, headerChars, maxHttpMessageSize);

				asyncTcpSocket.setEventHandler(connection);
				asyncTcpSocketImpl.register();

				if (inspector != null) inspector.onConnect(request, connection);

				connection.send(request, callback);
			}

			@Override
			public void onException(Exception e) {
				if (inspector != null) inspector.onConnectError(request, address, e);
				callback.setException(e);
			}

			@Override
			public String toString() {
				return "ConnectCallback for address: " + address.toString();
			}
		});
	}

	private static boolean isValidHost(String host, InetAddress inetAddress) {
		byte[] addressBytes = inetAddress.getAddress();
		if (addressBytes == null || addressBytes.length != 4)
			return false;
		// 0.0.0.*
		if (addressBytes[0] == 0 && addressBytes[1] == 0 && addressBytes[2] == 0)
			return false;
		return true;
	}

	public void close() {
		checkState(eventloop.inEventloopThread());
		if (closed)
			return;
		closed = true;
		if (expiredConnectionsCheck != null) {
			expiredConnectionsCheck.cancel();
			expiredConnectionsCheck = null;
		}

		ExposedLinkedList.Node<AbstractHttpConnection> node = keepAlivePool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();
			connection.close();
		}
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		callback.setComplete();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		close();
		callback.setComplete();
	}

	@JmxAttribute(description = "number of connections per address")
	public List<String> getAddressConnections() {
		if (keepAlivePoolsByAddresses.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("SocketAddress,ConnectionsCount");
		for (Entry<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> entry : keepAlivePoolsByAddresses.entrySet()) {
			InetSocketAddress address = entry.getKey();
			ExposedLinkedList<HttpClientConnection> connections = entry.getValue();
			result.add(address + "," + connections.size());
		}
		return result;
	}

	@JmxAttribute(
			description = "current number of connections in pool",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public long getConnectionsInPool() {
		return keepAlivePool.size();
	}

	@JmxAttribute
	public JmxInspector getStats() {
		return (inspector instanceof JmxInspector ? (JmxInspector) inspector : null);
	}

}
