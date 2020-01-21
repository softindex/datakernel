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

package io.datakernel.http;

import io.datakernel.async.service.EventloopService;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.MemSize;
import io.datakernel.common.inspector.AbstractInspector;
import io.datakernel.common.inspector.BaseInspector;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.DnsQueryException;
import io.datakernel.dns.DnsResponse;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.eventloop.jmx.ExceptionStats;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.jmx.api.JmxReducers.JmxReducerSum;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.net.AsyncTcpSocketImpl;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;
import static io.datakernel.eventloop.jmx.MBeanFormat.formatListAsMultilineString;
import static io.datakernel.http.AbstractHttpConnection.READ_TIMEOUT_ERROR;
import static io.datakernel.net.AsyncSslSocket.wrapClientSocket;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of {@link IAsyncHttpClient} that asynchronously connects
 * to real HTTP servers and gets responses from them.
 * <p>
 * It is also an {@link EventloopService} that needs its close method to be called
 * to cleanup the keepalive connections etc.
 */
@SuppressWarnings({"WeakerAccess", "unused", "UnusedReturnValue"})
public final class AsyncHttpClient implements IAsyncHttpClient, EventloopService, EventloopJmxMBeanEx {
	protected Logger logger = getLogger(getClass());

	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();
	public static final Duration CONNECT_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpClient.class, "connectTimeout", Duration.ZERO);
	public static final Duration READ_WRITE_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpClient.class, "readWriteTimeout", Duration.ZERO);
	public static final Duration READ_WRITE_TIMEOUT_SHUTDOWN = ApplicationSettings.getDuration(AsyncHttpClient.class, "readWriteTimeout_Shutdown", Duration.ofSeconds(3));
	public static final Duration KEEP_ALIVE_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpClient.class, "keepAliveTimeout", Duration.ZERO);
	public static final MemSize MAX_BODY_SIZE = ApplicationSettings.getMemSize(AsyncHttpClient.class, "maxBodySize", MemSize.ZERO);
	public static final int MAX_KEEP_ALIVE_REQUESTS = ApplicationSettings.getInt(AsyncHttpClient.class, "maxKeepAliveRequests", 0);

	@NotNull
	private final Eventloop eventloop;
	@NotNull
	private AsyncDnsClient asyncDnsClient;
	@NotNull
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	final HashMap<InetSocketAddress, AddressLinkedList> addresses = new HashMap<>();
	final ConnectionsLinkedList poolKeepAlive = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolReadWrite = new ConnectionsLinkedList();
	private int poolKeepAliveExpired;
	private int poolReadWriteExpired;

	@Nullable
	private ScheduledRunnable expiredConnectionsCheck;

	// timeouts
	int connectTimeoutMillis = (int) CONNECT_TIMEOUT.toMillis();
	int readWriteTimeoutMillis = (int) READ_WRITE_TIMEOUT.toMillis();
	int readWriteTimeoutMillisShutdown = (int) READ_WRITE_TIMEOUT_SHUTDOWN.toMillis();
	int keepAliveTimeoutMillis = (int) KEEP_ALIVE_TIMEOUT.toMillis();
	int maxBodySize = MAX_BODY_SIZE.toInt();
	int maxKeepAliveRequests = MAX_KEEP_ALIVE_REQUESTS;

	// SSL
	private SSLContext sslContext;
	private Executor sslExecutor;

	@Nullable
	private AsyncTcpSocketImpl.Inspector socketInspector;
	@Nullable
	private AsyncTcpSocketImpl.Inspector socketSslInspector;
	@Nullable
	Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onRequest(HttpRequest request);

		void onResolve(HttpRequest request, DnsResponse dnsResponse);

		void onResolveError(HttpRequest request, Throwable e);

		void onConnect(HttpRequest request, HttpClientConnection connection);

		void onConnectError(HttpRequest request, InetSocketAddress address, Throwable e);

		void onHttpResponse(HttpClientConnection connection, HttpResponse response);

		void onHttpError(HttpClientConnection connection, boolean keepAliveConnection, Throwable e);
	}

	@SuppressWarnings("WeakerAccess")
	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

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
		public void onRequest(HttpRequest request) {
			totalRequests.recordEvent();
		}

		@Override
		public void onResolve(HttpRequest request, DnsResponse dnsResponse) {
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

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getActiveRequests() {
			return totalRequests.getTotalCount() -
					(httpTimeouts.getTotalCount() + resolveErrors.getTotal() + connectErrors.getTotal() + responsesErrors + responses);
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
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
	private AsyncHttpClient(@NotNull Eventloop eventloop, @NotNull AsyncDnsClient asyncDnsClient) {
		this.eventloop = eventloop;
		this.asyncDnsClient = asyncDnsClient;
	}

	public static AsyncHttpClient create(@NotNull Eventloop eventloop) {
		AsyncDnsClient defaultDnsClient = RemoteAsyncDnsClient.create(eventloop);
		return new AsyncHttpClient(eventloop, defaultDnsClient);
	}

	public AsyncHttpClient withSocketSettings(@NotNull SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	public AsyncHttpClient withDnsClient(@NotNull AsyncDnsClient asyncDnsClient) {
		this.asyncDnsClient = asyncDnsClient;
		return this;
	}

	public AsyncHttpClient withSslEnabled(@NotNull SSLContext sslContext, @NotNull Executor sslExecutor) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		return this;
	}

	public AsyncHttpClient withKeepAliveTimeout(@NotNull Duration keepAliveTime) {
		this.keepAliveTimeoutMillis = (int) keepAliveTime.toMillis();
		return this;
	}

	public AsyncHttpClient withNoKeepAlive() {
		return withKeepAliveTimeout(Duration.ZERO);
	}

	public AsyncHttpClient withMaxKeepAliveRequests(int maxKeepAliveRequests) {
		checkArgument(maxKeepAliveRequests >= 0, "Maximum number of requests per keep-alive connection should not be less than zero");
		this.maxKeepAliveRequests = maxKeepAliveRequests;
		return this;
	}

	public AsyncHttpClient withReadWriteTimeout(@NotNull Duration readTimeout) {
		this.readWriteTimeoutMillis = (int) readTimeout.toMillis();
		return this;
	}

	public AsyncHttpClient withConnectTimeout(@NotNull Duration connectTimeout) {
		this.connectTimeoutMillis = (int) connectTimeout.toMillis();
		return this;
	}

	public AsyncHttpClient withMaxBodySize(MemSize maxBodySize) {
		return withMaxBodySize(maxBodySize.toInt());
	}

	public AsyncHttpClient withMaxBodySize(int maxBodySize) {
		this.maxBodySize = maxBodySize != 0 ? maxBodySize : Integer.MAX_VALUE;
		return this;
	}

	public AsyncHttpClient withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	public AsyncHttpClient withSocketInspector(AsyncTcpSocketImpl.Inspector socketInspector) {
		this.socketInspector = socketInspector;
		return this;
	}

	public AsyncHttpClient withSocketSslInspector(AsyncTcpSocketImpl.Inspector socketSslInspector) {
		this.socketSslInspector = socketSslInspector;
		return this;
	}
	// endregion

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.delayBackground(1000L, wrapContext(this, () -> {
			expiredConnectionsCheck = null;
			poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
			boolean isClosing = closePromise != null;
			if (readWriteTimeoutMillis != 0 || isClosing) {
				poolReadWriteExpired += poolReadWrite.closeExpiredConnections(eventloop.currentTimeMillis() -
						(!isClosing ? readWriteTimeoutMillis : readWriteTimeoutMillisShutdown), READ_TIMEOUT_ERROR);
			}
			if (getConnectionsCount() != 0) {
				scheduleExpiredConnectionsCheck();
				if (isClosing) {
					logger.info("...Waiting for " + this);
				}
			}
		}));
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
		//noinspection AssertWithSideEffects,ConstantConditions
		assert (connection.pool = null) == null;
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
		connection.switchPool(poolKeepAlive);

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	@Override
	public Promise<HttpResponse> request(HttpRequest request) {
		assert eventloop.inEventloopThread();
		if (inspector != null) inspector.onRequest(request);
		String host = request.getUrl().getHost();

		assert host != null;

		return asyncDnsClient.resolve4(host)
				.thenEx((dnsResponse, e) -> {
					if (e == null) {
						if (inspector != null) inspector.onResolve(request, dnsResponse);
						if (dnsResponse.isSuccessful()) {
							//noinspection ConstantConditions - dnsResponse is successful (not null)
							return doSend(request, dnsResponse.getRecord().getIps());
						} else {
							return Promise.ofException(new DnsQueryException(AsyncHttpClient.class, dnsResponse));
						}
					} else {
						if (inspector != null) inspector.onResolveError(request, e);
						request.recycle();
						return Promise.ofException(e);
					}
				});
	}

	private Promise<HttpResponse> doSend(HttpRequest request, InetAddress[] inetAddresses) {
		InetAddress inetAddress = inetAddresses[(inetAddressIdx++ & Integer.MAX_VALUE) % inetAddresses.length];
		InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		HttpClientConnection keepAliveConnection = takeKeepAliveConnection(address);
		if (keepAliveConnection != null) {
			return keepAliveConnection.send(request);
		}

		return AsyncTcpSocketImpl.connect(address, connectTimeoutMillis, socketSettings)
				.thenEx((asyncTcpSocketImpl, e) -> {
					if (e == null) {
						boolean https = request.isHttps();
						asyncTcpSocketImpl
								.withInspector(https ? socketInspector : socketSslInspector);

						if (https && sslContext == null) {
							throw new IllegalArgumentException("Cannot send HTTPS Request without SSL enabled");
						}

						String host = request.getUrl().getHost();
						assert host != null;

						AsyncTcpSocket asyncTcpSocket = https ?
								wrapClientSocket(asyncTcpSocketImpl,
										host, request.getUrl().getPort(),
										sslContext, sslExecutor) :
								asyncTcpSocketImpl;

						HttpClientConnection connection = new HttpClientConnection(eventloop, this, asyncTcpSocket, address);

						if (inspector != null) inspector.onConnect(request, connection);

						if (expiredConnectionsCheck == null)
							scheduleExpiredConnectionsCheck();

						return connection.send(request);
					} else {
						if (inspector != null) inspector.onConnectError(request, address, e);
						request.recycle();
						return Promise.ofException(e);
					}
				});
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		return Promise.complete();
	}

	@Nullable
	private SettablePromise<Void> closePromise;

	public void onConnectionClosed() {
		if (getConnectionsCount() == 0 && closePromise != null) {
			closePromise.set(null);
			closePromise = null;
		}
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		checkState(eventloop.inEventloopThread(), "Not in eventloop thread");

		SettablePromise<Void> promise = new SettablePromise<>();

		poolKeepAlive.closeAllConnections();
		assert addresses.isEmpty();
		keepAliveTimeoutMillis = 0;
		if (getConnectionsCount() == 0) {
			assert poolReadWrite.isEmpty();
			promise.set(null);
		} else {
			closePromise = promise;
			logger.info("Waiting for " + this);
		}
		return promise;
	}

	// region jmx
	@JmxAttribute(description = "current number of connections", reducer = JmxReducerSum.class)
	public int getConnectionsCount() {
		return poolKeepAlive.size() + poolReadWrite.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveCount() {
		return poolKeepAlive.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadWriteCount() {
		return poolReadWrite.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveExpired() {
		return poolKeepAliveExpired;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadWriteExpired() {
		return poolReadWriteExpired;
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

	@JmxAttribute
	@Nullable
	public AsyncTcpSocketImpl.JmxInspector getSocketStats() {
		return BaseInspector.lookup(socketInspector, AsyncTcpSocketImpl.JmxInspector.class);
	}

	@JmxAttribute
	@Nullable
	public AsyncTcpSocketImpl.JmxInspector getSocketStatsSsl() {
		return BaseInspector.lookup(socketSslInspector, AsyncTcpSocketImpl.JmxInspector.class);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}
	// endregion

	@Override
	public String toString() {
		return "AsyncHttpClient" + "{" + "read/write:" + poolReadWrite.size() + " keep-alive:" + poolKeepAlive.size() + "}";
	}
}
