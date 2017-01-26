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
import static io.datakernel.http.AbstractHttpConnection.*;
import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public final class AsyncHttpClient implements IAsyncHttpClient, EventloopService, EventloopJmxMBean {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create();
	public static final long DEFAULT_KEEP_ALIVE_MILLIS = 30 * 1000L;

	private final Eventloop eventloop;
	private IAsyncDnsClient asyncDnsClient;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	int connectionsCount;
	final HashMap<InetSocketAddress, AddressLinkedList> addresses = new HashMap<>();
	final ConnectionsLinkedList poolKeepAlive = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolReading = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolWriting = new ConnectionsLinkedList();
	private int poolKeepAliveExpired;
	private int poolReadingExpired;
	private int poolWritingExpired;

	private final char[] headerChars = new char[MAX_HEADER_LINE_SIZE];

	private AsyncCancellable expiredConnectionsCheck;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	// timeouts
	private int connectTimeoutMillis = 0;
	int keepAliveTimeoutMillis = (int) DEFAULT_KEEP_ALIVE_MILLIS;
	private int readTimeoutMillis = 0;
	private int writeTimeoutMillis = 0;

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

	public AsyncHttpClient withKeepAliveTimeout(long keepAliveTimeMillis) {
		this.keepAliveTimeoutMillis = (int) keepAliveTimeMillis;
		return this;
	}

	public AsyncHttpClient withNoKeepAlive() {
		return withKeepAliveTimeout(0);
	}

	public AsyncHttpClient withReadTimeout(long readTimeoutMillis) {
		this.readTimeoutMillis = (int) readTimeoutMillis;
		return this;
	}

	public AsyncHttpClient withWriteTimeout(long writeTimeoutMillis) {
		this.writeTimeoutMillis = (int) writeTimeoutMillis;
		return this;
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
		expiredConnectionsCheck = eventloop.scheduleBackground(eventloop.currentTimeMillis() + 1000L, new ScheduledRunnable() {
			@Override
			public void run() {
				expiredConnectionsCheck = null;
				poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
				if (readTimeoutMillis != 0)
					poolReadingExpired += poolReading.closeExpiredConnections(eventloop.currentTimeMillis() - readTimeoutMillis, READ_TIMEOUT_ERROR);
				if (writeTimeoutMillis != 0)
					poolWritingExpired += poolWriting.closeExpiredConnections(eventloop.currentTimeMillis() - writeTimeoutMillis, WRITE_TIMEOUT_ERROR);
				if (connectionsCount != 0)
					scheduleExpiredConnectionsCheck();
			}
		});
	}

	private HttpClientConnection takeKeepAliveConnection(InetSocketAddress address) {
		AddressLinkedList addresses = this.addresses.get(address);
		if (addresses == null)
			return null;
		HttpClientConnection connection = addresses.removeLastNode();
		assert connection.pool == poolKeepAlive;
		assert connection.remoteAddress.equals(address);
		connection.pool.removeNode(connection);
		connection.pool = null;
		if (addresses.isEmpty()) {
			this.addresses.remove(address);
		}
		return connection;
	}

	void returnToKeepAlivePool(HttpClientConnection connection) {
		assert !connection.isClosed();
		AddressLinkedList addresses = this.addresses.get(connection.remoteAddress);
		if (addresses == null) {
			addresses = new AddressLinkedList();
			this.addresses.put(connection.remoteAddress, addresses);
		}
		addresses.addLastNode(connection);
		assert connection.pool == poolReading;
		poolReading.removeNode(connection);
		(connection.pool = poolKeepAlive).addLastNode(connection);
		connection.poolTimestamp = eventloop.currentTimeMillis();

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
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

	private void getUrlForHostAsync(final HttpRequest request, final InetAddress[] inetAddresses,
	                                final ResultCallback<HttpResponse> callback) {
		String host = request.getUrl().getHost();
		final InetAddress inetAddress = inetAddresses[((inetAddressIdx++) & Integer.MAX_VALUE) % inetAddresses.length];
		if (!isValidHost(host, inetAddress)) {
			callback.setException(new IOException("Invalid IP address " + inetAddress + " for host " + host));
			return;
		}

		final InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		HttpClientConnection connection = takeKeepAliveConnection(address);
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

				HttpClientConnection connection = new HttpClientConnection(eventloop, address, asyncTcpSocket,
						AsyncHttpClient.this, headerChars, maxHttpMessageSize);

				asyncTcpSocket.setEventHandler(connection);
				asyncTcpSocketImpl.register();

				if (inspector != null) inspector.onConnect(request, connection);

				connectionsCount++;
				if (expiredConnectionsCheck == null)
					scheduleExpiredConnectionsCheck();

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

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		callback.setComplete();
	}

	private CompletionCallback closeCallback;

	public void onConnectionClosed() {
		connectionsCount--;
		if (connectionsCount == 0 && closeCallback != null) {
			closeCallback.postComplete(eventloop);
			closeCallback = null;
		}
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		poolKeepAlive.closeAllConnections();
		assert addresses.isEmpty();
		keepAliveTimeoutMillis = 0;
		if (connectionsCount == 0) {
			assert poolReading.isEmpty() && poolWriting.isEmpty();
			callback.postComplete(eventloop);
		} else {
			this.closeCallback = callback;
		}
	}

	@JmxAttribute(description = "current number of connections", reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsCount() {
		return connectionsCount;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsKeepAliveCount() {
		return poolKeepAlive.size();
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsReadingCount() {
		return poolReading.size();
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsWritingCount() {
		return poolWriting.size();
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsKeepAliveExpired() {
		return poolKeepAliveExpired;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsReadingExpired() {
		return poolReadingExpired;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsWritingExpired() {
		return poolWritingExpired;
	}

	@JmxAttribute(description = "number of connections per address")
	public List<String> getAddressConnections() {
		if (addresses.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("SocketAddress,ConnectionsCount");
		for (Entry<InetSocketAddress, AddressLinkedList> entry : addresses.entrySet()) {
			InetSocketAddress address = entry.getKey();
			AddressLinkedList connections = entry.getValue();
			result.add(address + "," + connections.size());
		}
		return result;
	}

	@JmxAttribute
	public JmxInspector getStats() {
		return (inspector instanceof JmxInspector ? (JmxInspector) inspector : null);
	}

}
