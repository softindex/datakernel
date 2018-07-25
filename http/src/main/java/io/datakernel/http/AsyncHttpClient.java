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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.IAsyncDnsClient;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.http.AbstractHttpConnection.*;
import static io.datakernel.jmx.MBeanFormat.formatListAsMultilineString;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public final class AsyncHttpClient implements IAsyncHttpClient, EventloopService, EventloopJmxMBeanEx {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create();
	public static final Duration DEFAULT_KEEP_ALIVE_MILLIS = Duration.ofSeconds(30);

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

	private final char[] headerChars = new char[MAX_HEADER_LINE_SIZE.toInt()];

	@Nullable
	private AsyncCancellable expiredConnectionsCheck;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	// timeouts
	private int connectTimeoutMillis = 0;
	int keepAliveTimeoutMillis = (int) DEFAULT_KEEP_ALIVE_MILLIS.getSeconds();
	int maxKeepAliveRequests = -1;
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

		void onResolveError(HttpRequest request, Throwable e);

		void onConnect(HttpRequest request, HttpClientConnection connection);

		void onConnectError(HttpRequest request, InetSocketAddress address, Throwable e);

		void onHttpResponse(HttpClientConnection connection, HttpResponse response);

		void onHttpError(HttpClientConnection connection, boolean keepAliveConnection, Throwable e);
	}

	public static class JmxInspector implements Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		protected final AsyncTcpSocketImpl.JmxInspector socketStats = new AsyncTcpSocketImpl.JmxInspector();
		protected final AsyncTcpSocketImpl.JmxInspector socketStatsForSSL = new AsyncTcpSocketImpl.JmxInspector();
		private final EventStats totalRequests = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats resolveErrors = ExceptionStats.create();
		private final EventStats connected = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats connectErrors = ExceptionStats.create();
		private long responses;
		private final EventStats httpTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats httpErrors = ExceptionStats.create();
		private long responsesErrors;
		private final EventStats sslErrors = EventStats.create(SMOOTHING_WINDOW);

		@Override
		public AsyncTcpSocketImpl.Inspector socketInspector(HttpRequest httpRequest, InetSocketAddress address, boolean https) {
			return https ? socketStatsForSSL : socketStats;
		}

		@Override
		public void onRequest(HttpRequest request) {
			totalRequests.recordEvent();
		}

		@Override
		public void onResolve(HttpRequest request, InetAddress[] inetAddresses) {
		}

		@Override
		public void onResolveError(HttpRequest request, Throwable e) {
			resolveErrors.recordException(e, request.getUrl().getHost());
		}

		@Override
		public void onConnect(HttpRequest request, HttpClientConnection connection) {
			connected.recordEvent();
		}

		@Override
		public void onConnectError(HttpRequest request, InetSocketAddress address, Throwable e) {
			connectErrors.recordException(e, request.getUrl().getHost());
		}

		@Override
		public void onHttpResponse(HttpClientConnection connection, HttpResponse response) {
			responses++;
		}

		@Override
		public void onHttpError(HttpClientConnection connection, boolean keepAliveConnection, Throwable e) {
			if (e == AbstractHttpConnection.READ_TIMEOUT_ERROR || e == AbstractHttpConnection.WRITE_TIMEOUT_ERROR) {
				httpTimeouts.recordEvent();
				return;
			}
			httpErrors.recordException(e);
			if (SSLException.class == e.getClass()) {
				sslErrors.recordEvent();
			}
			// when connection is in keep-alive state, it means that the response already happenned,
			// so error of keep-alive connection is not a response error
			if (!keepAliveConnection) {
				responsesErrors++;
			}
		}

		@JmxAttribute
		public AsyncTcpSocketImpl.JmxInspector getSocketStats() {
			return socketStats;
		}

		@JmxAttribute(extraSubAttributes = "totalCount", description = "all requests that were sent (both successful and failed)")
		public EventStats getTotalRequests() {
			return totalRequests;
		}

		@JmxAttribute
		public ExceptionStats getResolveErrors() {
			return resolveErrors;
		}

		@JmxAttribute
		public ExceptionStats getConnectErrors() {
			return connectErrors;
		}

		@JmxAttribute(description = "number of \"open connection\" events)")
		public EventStats getConnected() {
			return connected;
		}

		@JmxAttribute
		public EventStats getHttpTimeouts() {
			return httpTimeouts;
		}

		@JmxAttribute
		public ExceptionStats getHttpErrors() {
			return httpErrors;
		}

		@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
		public long getActiveRequests() {
			return totalRequests.getTotalCount() -
					(httpTimeouts.getTotalCount() + resolveErrors.getTotal() + connectErrors.getTotal() + responsesErrors + responses);
		}

		@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
		public long getTotalResponses() {
			return responses;
		}

		@JmxAttribute
		public EventStats getSslErrors() {
			return sslErrors;
		}

	}

	private int inetAddressIdx = 0;

	// region builders
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

	public AsyncHttpClient withKeepAliveTimeout(Duration keepAliveTime) {
		this.keepAliveTimeoutMillis = (int) keepAliveTime.toMillis();
		return this;
	}

	public AsyncHttpClient withNoKeepAlive() {
		return withKeepAliveTimeout(Duration.ZERO);
	}

	public AsyncHttpClient  withMaxKeepAliveRequests(int maxKeepAliveRequests) {
		checkArgument(maxKeepAliveRequests >= 0, "Maximum number of requests per keep-alive connection should not be less than zero");
		this.maxKeepAliveRequests = maxKeepAliveRequests;
		return this;
	}

	public AsyncHttpClient withReadTimeout(Duration readTimeout) {
		this.readTimeoutMillis = (int) readTimeout.toMillis();
		return this;
	}

	public AsyncHttpClient withWriteTimeout(Duration writeTimeout) {
		this.writeTimeoutMillis = (int) writeTimeout.toMillis();
		return this;
	}

	public AsyncHttpClient withConnectTimeout(Duration connectTimeout) {
		this.connectTimeoutMillis = (int) connectTimeout.toMillis();
		return this;
	}

	public AsyncHttpClient withMaxHttpMessageSize(@Nullable MemSize maxHttpMessageSize) {
		this.maxHttpMessageSize = maxHttpMessageSize != null ? maxHttpMessageSize.toInt() : Integer.MAX_VALUE;
		return this;
	}

	public AsyncHttpClient withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}
	// endregion

	@SuppressWarnings("Duplicates")
	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.delayBackground(1000L, () -> {
			expiredConnectionsCheck = null;
			poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
			if (readTimeoutMillis != 0)
				poolReadingExpired += poolReading.closeExpiredConnections(eventloop.currentTimeMillis() - readTimeoutMillis, READ_TIMEOUT_ERROR);
			if (writeTimeoutMillis != 0)
				poolWritingExpired += poolWriting.closeExpiredConnections(eventloop.currentTimeMillis() - writeTimeoutMillis, WRITE_TIMEOUT_ERROR);
			if (connectionsCount != 0)
				scheduleExpiredConnectionsCheck();
		});
	}

	@Nullable
	private HttpClientConnection takeKeepAliveConnection(InetSocketAddress address) {
		AddressLinkedList addresses = this.addresses.get(address);
		if (addresses == null)
			return null;
		HttpClientConnection connection = addresses.removeLastNode();
		assert connection != null;
		assert connection.pool == poolKeepAlive;
		assert connection.remoteAddress.equals(address);
		connection.pool.removeNode(connection); // moving from keep-alive state to taken(null) state
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
		poolReading.removeNode(connection); // moving from reading state to keepalive state
		(connection.pool = poolKeepAlive).addLastNode(connection);
		connection.poolTimestamp = eventloop.currentTimeMillis();

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	/**
	 * Sends the request to server, waits the result timeout and handles result with callback
	 *
	 * @param request request for server
	 */
	@Override
	public void send(HttpRequest request, Callback<HttpResponse> callback) {
		assert eventloop.inEventloopThread();
		if (inspector != null) inspector.onRequest(request);
		String host = request.getUrl().getHost();

		assert host != null;

		asyncDnsClient.resolve4(host, new Callback<InetAddress[]>() {
			@Override
			public void set(InetAddress[] inetAddresses) {
				if (inspector != null) inspector.onResolve(request, inetAddresses);
				doSend(request, inetAddresses, callback);
			}

			@Override
			public void setException(Throwable e) {
				if (inspector != null) inspector.onResolveError(request, e);
				request.recycleBufs();
				callback.setException(e);
			}
		});
	}

	private void doSend(HttpRequest request, InetAddress[] inetAddresses,
			Callback<HttpResponse> callback) {
		InetAddress inetAddress = inetAddresses[((inetAddressIdx++) & Integer.MAX_VALUE) % inetAddresses.length];
		InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		HttpClientConnection keepAliveConnection = takeKeepAliveConnection(address);
		if (keepAliveConnection != null) {
			keepAliveConnection.send(request, callback);
			return;
		}

		eventloop.connect(address, connectTimeoutMillis).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				boolean https = request.isHttps();
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings)
						.withInspector(inspector == null ? null : inspector.socketInspector(request, address, https));

				if (https && sslContext == null) {
					throw new IllegalArgumentException("Cannot send HTTPS Request without SSL enabled");
				}

				String host = request.getUrl().getHost();
				assert host != null;

				AsyncTcpSocket asyncTcpSocket = https ?
						wrapClientSocket(eventloop, asyncTcpSocketImpl,
								host, request.getUrl().getPort(),
								sslContext, sslExecutor) :
						asyncTcpSocketImpl;

				HttpClientConnection connection = new HttpClientConnection(eventloop, address, asyncTcpSocket,
						AsyncHttpClient.this, headerChars, maxHttpMessageSize);

				asyncTcpSocket.setEventHandler(connection);
				asyncTcpSocketImpl.register();

				if (inspector != null) inspector.onConnect(request, connection);

				connectionsCount++;
				if (expiredConnectionsCheck == null)
					scheduleExpiredConnectionsCheck();

				// connection was unexpectedly closed by the peer
				if (connection.getCloseError() != null) {
					callback.setException(connection.getCloseError());
					return;
				}

				connection.send(request, callback);
			} else {
				if (inspector != null) inspector.onConnectError(request, address, throwable);
				request.recycleBufs();
				callback.setException(throwable);
			}
		});
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Stage<Void> start() {
		checkState(eventloop.inEventloopThread());
		return Stage.of(null);
	}

	@Nullable
	private SettableStage<Void> closeStage;

	public void onConnectionClosed() {
		connectionsCount--;
		if (connectionsCount == 0 && closeStage != null) {
			closeStage.set(null);
			closeStage = null;
		}
	}

	@Override
	public Stage<Void> stop() {
		checkState(eventloop.inEventloopThread());
		SettableStage<Void> stage = new SettableStage<>();

		poolKeepAlive.closeAllConnections();
		assert addresses.isEmpty();
		keepAliveTimeoutMillis = 0;
		if (connectionsCount == 0) {
			assert poolReading.isEmpty() && poolWriting.isEmpty();
			stage.set(null);
		} else {
			this.closeStage = stage;
		}
		return stage;
	}

	// region jmx
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

	@JmxOperation(description = "number of connections per address")
	public String getAddressConnections() {
		if (addresses.isEmpty())
			return "";
		List<String> result = new ArrayList<>();
		result.add("SocketAddress,ConnectionsCount");
		for (Entry<InetSocketAddress, AddressLinkedList> entry : addresses.entrySet()) {
			InetSocketAddress address = entry.getKey();
			AddressLinkedList connections = entry.getValue();
			result.add(address + ", " + connections.size());
		}
		return formatListAsMultilineString(result);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return (inspector instanceof JmxInspector ? (JmxInspector) inspector : null);
	}
	// endregion
}
