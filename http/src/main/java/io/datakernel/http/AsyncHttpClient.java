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

import io.datakernel.async.*;
import io.datakernel.dns.DnsClient;
import io.datakernel.eventloop.*;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.jmx.MBeanFormat.formatDuration;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public class AsyncHttpClient implements EventloopService, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static final long CHECK_PERIOD = 1000L;
	private static final long DEFAULT_KEEP_CONNECTION_IN_POOL_TIME = 30 * 1000L;

	private final Eventloop eventloop;
	private final DnsClient dnsClient;
	private final SocketSettings socketSettings;
	private final ExposedLinkedList<AbstractHttpConnection> pool;
	private final HashMap<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> addressPools = new HashMap<>();
	private final char[] headerChars;

	private AsyncCancellable expiredConnectionsCheck;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	// timeouts
	private long keepConnectionInPoolTime = DEFAULT_KEEP_CONNECTION_IN_POOL_TIME;

	// SSL
	private SSLContext sslContext;
	private ExecutorService sslExecutor;

	// jmx
	private final EventStats totalRequests = EventStats.create();
	private final EventStats httpsRequests = EventStats.create();
	private final EventStats httpRequests = EventStats.create();
	private final EventStats keepAliveRequests = EventStats.create();
	private final EventStats nonKeepAliveRequests = EventStats.create();
	private final EventStats expiredConnections = EventStats.create();
	private final ExceptionStats httpProtocolErrors = ExceptionStats.create();
	private final EventStats timeoutErrors = EventStats.create();
	private final Map<HttpClientConnection, UrlWithTimestamp> currentRequestToSendTime = new HashMap<>();
	private boolean monitorCurrentRequestsDuration = false;

	private int inetAddressIdx = 0;

	private AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient) {
		this(eventloop, dnsClient, defaultSocketSettings());
	}

	public static AsyncHttpClient create(Eventloop eventloop, DnsClient dnsClient) {
		return new AsyncHttpClient(eventloop, dnsClient);
	}

	private AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient, SocketSettings socketSettings) {
		this.eventloop = eventloop;
		this.dnsClient = dnsClient;
		this.socketSettings = checkNotNull(socketSettings);
		this.pool = ExposedLinkedList.create();
		char[] chars = eventloop.get(char[].class);
		if (chars == null || chars.length < MAX_HEADER_LINE_SIZE) {
			chars = new char[MAX_HEADER_LINE_SIZE];
			eventloop.set(char[].class, chars);
		}
		this.headerChars = chars;
	}

	public AsyncHttpClient withSslEnabled(SSLContext sslContext, ExecutorService executor) {
		this.sslContext = sslContext;
		this.sslExecutor = executor;
		return this;
	}

	public AsyncHttpClient withKeepConnectionAliveTime(long time) {
		this.keepConnectionInPoolTime = time;
		return this;
	}

	public AsyncHttpClient withMaxHttpMessageSize(int size) {
		this.maxHttpMessageSize = size;
		return this;
	}

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.scheduleBackground(eventloop.currentTimeMillis() + CHECK_PERIOD, new Runnable() {
			@Override
			public void run() {
				expiredConnectionsCheck = null;
				checkExpiredConnections();
				if (!pool.isEmpty()) {
					scheduleExpiredConnectionsCheck();
				}
			}
		});
	}

	private int checkExpiredConnections() {
		int count = 0;
		final long now = eventloop.currentTimeMillis();

		ExposedLinkedList.Node<AbstractHttpConnection> node = pool.getFirstNode();
		while (node != null) {
			HttpClientConnection connection = (HttpClientConnection) node.getValue();
			node = node.getNext();

			long idleTime = now - connection.poolTimestamp;
			if (idleTime < keepConnectionInPoolTime)
				break; // connections must back ordered by activity
			connection.close();
			count++;
		}
		expiredConnections.recordEvents(count);
		return count;
	}

	private HttpClientConnection takeConnection(InetSocketAddress address) {
		ExposedLinkedList<HttpClientConnection> addressPool = addressPools.get(address);
		if (addressPool == null)
			return null;
		HttpClientConnection connection = addressPool.getFirstValue();
		if (connection == null)
			return null;
		removeFromPool(connection);
		return connection;
	}

	void addToPool(HttpClientConnection connection) {
		assert !connection.isClosed();
		assert !connection.isInPool();

		ExposedLinkedList<HttpClientConnection> addressPool = connection.addressPool;
		if (addressPool == null) {
			addressPool = addressPools.get(connection.remoteAddress);
			if (addressPool == null) {
				addressPool = ExposedLinkedList.create();
				addressPools.put(connection.remoteAddress, addressPool);
			}
			connection.addressPool = addressPool;
		}
		addressPool.addLastNode(connection.addressNode);
		pool.addLastNode(connection.poolNode);
		connection.poolTimestamp = eventloop.currentTimeMillis();

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	void removeFromPool(HttpClientConnection connection) {
		assert connection.isInPool();
		pool.removeNode(connection.poolNode);
		connection.addressPool.removeNode(connection.addressNode);
		if (connection.addressPool.isEmpty()) {
			addressPools.remove(connection.remoteAddress);
			connection.addressPool = null;
		}
		connection.poolTimestamp = 0L;
	}

	/**
	 * Sends the request to server, waits the result timeout and handles result with callback
	 *
	 * @param request  request for server
	 * @param timeout  time which client will wait result
	 * @param callback callback for handling result
	 */
	public void send(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		assert eventloop.inEventloopThread();

		totalRequests.recordEvent();
		getUrlAsync(request, timeout, callback);
	}

	private void getUrlAsync(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		dnsClient.resolve4(request.getUrl().getHost(), new ForwardingResultCallback<InetAddress[]>(callback) {
			@Override
			public void onResult(InetAddress[] inetAddresses) {
				getUrlForHostAsync(request, timeout, inetAddresses, callback);
			}
		});
	}

	private InetAddress getNextInetAddress(InetAddress[] inetAddresses) {
		return inetAddresses[((inetAddressIdx++) & Integer.MAX_VALUE) % inetAddresses.length];
	}

	private void getUrlForHostAsync(final HttpRequest request, int timeout, final InetAddress[] inetAddresses, final ResultCallback<HttpResponse> callback) {
		String host = request.getUrl().getHost();
		final InetAddress inetAddress = getNextInetAddress(inetAddresses);
		if (!isValidHost(host, inetAddress)) {
			callback.onException(new IOException("Invalid IP address " + inetAddress + " for host " + host));
			return;
		}

		final long timeoutTime = eventloop.currentTimeMillis() + timeout;
		final InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		HttpClientConnection connection = takeConnection(address);
		if (connection != null) {
			sendRequest(connection, request, timeoutTime, callback);
			return;
		}

		eventloop.connect(address, timeout, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
				AsyncTcpSocket asyncTcpSocket = request.isHttps() ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;

				HttpClientConnection connection = HttpClientConnection.create(eventloop, address,
						asyncTcpSocket,
						AsyncHttpClient.this, headerChars, maxHttpMessageSize);

				asyncTcpSocket.setEventHandler(connection);
				asyncTcpSocketImpl.register();

				sendRequest(connection, request, timeoutTime, callback);
			}

			@Override
			public void onException(Exception exception) {
				logger.trace("eventloop.connect.onException Calling {}", request);

				if (logger.isWarnEnabled()) {
					logger.warn("Connect error to {} : {}", address, exception.getMessage());
				}
				callback.onException(exception);
			}
		});
	}

	private void sendRequest(final HttpClientConnection connection, HttpRequest request, long timeoutTime, final ResultCallback<HttpResponse> callback) {
		logger.trace("sendRequest Calling {}", request);

		// jmx
		ResultCallback<HttpResponse> responseCallback = callback;
		if (monitorCurrentRequestsDuration) {
			currentRequestToSendTime.put(
					connection,
					new UrlWithTimestamp(request.getFullUrl(), eventloop.currentTimeMillis())
			);
			responseCallback = new MonitorRequestDurationCallback(callback, connection);
		}

		connection.send(request, timeoutTime, responseCallback);
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
		if (expiredConnectionsCheck != null) {
			expiredConnectionsCheck.cancel();
			expiredConnectionsCheck = null;
		}

		ExposedLinkedList.Node<AbstractHttpConnection> node = pool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			// TODO: close() -> closeWithError() ?
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
		callback.onComplete();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		close();
		callback.onComplete();
	}

	public CompletionCallbackFuture closeFuture() {
		return AsyncCallbacks.stopFuture(this);
	}

	// jmx
	void recordRequestEvent(boolean https, boolean keepAlive) {
		totalRequests.recordEvent();

		if (https) {
			httpsRequests.recordEvent();
		} else {
			httpRequests.recordEvent();
		}

		if (keepAlive) {
			keepAliveRequests.recordEvent();
		} else {
			nonKeepAliveRequests.recordEvent();
		}
	}

	void recordHttpProtocolError(Throwable e, String url) {
		httpProtocolErrors.recordException(e, "url: " + url);
	}

	void recordTimeoutError() {
		timeoutErrors.recordEvent();
	}

	@JmxAttribute(
			description = "current number of connections",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getConnectionsCount() {
		return pool.size();
	}

	@JmxAttribute(description = "number of connections per address")
	public List<String> getAddressConnections() {
		if (addressPools.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("SocketAddress,ConnectionsCount");
		for (Entry<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> entry : addressPools.entrySet()) {
			InetSocketAddress address = entry.getKey();
			ExposedLinkedList<HttpClientConnection> connections = entry.getValue();
			result.add(address + "," + connections.size());
		}
		return result;
	}

	@JmxAttribute(description = "all requests that were sent (both successful and failed)")
	public EventStats getTotalRequests() {
		return totalRequests;
	}

	@JmxAttribute(description = "successful requests that were sent over secured connection (https)")
	public EventStats getHttpsRequests() {
		return httpsRequests;
	}

	@JmxAttribute(description = "successful requests that were sent over unsecured connection (http)")
	public EventStats getHttpRequests() {
		return httpRequests;
	}

	@JmxAttribute(description = "after receiving response for this type of request connection is returned to pool " +
			"(request was successful)")
	public EventStats getKeepAliveRequests() {
		return keepAliveRequests;
	}

	@JmxAttribute(description = "after receiving response for this type of request connection is closed " +
			"(request was successful)")
	public EventStats getNonKeepAliveRequests() {
		return nonKeepAliveRequests;
	}

	@JmxAttribute(description = "number of expired connections in keep-alive pool (after appropriate timeout)")
	public EventStats getExpiredConnections() {
		return expiredConnections;
	}

	@JmxAttribute(description = "Number of HTTP responses which could not be parsed " +
			"according to http protocol-specific errors (failed requests)")
	public ExceptionStats getHttpProtocolErrors() {
		return httpProtocolErrors;
	}

	@JmxAttribute(description = "Number of requests that didn't receive response in specified timeout " +
			"(failed requests)")
	public EventStats getTimeoutErrors() {
		return timeoutErrors;
	}

	@JmxAttribute
	public boolean isMonitorCurrentRequestsDuration() {
		return monitorCurrentRequestsDuration;
	}

	@JmxAttribute
	public void setMonitorCurrentRequestsDuration(boolean monitor) {
		if (!monitor) {
			currentRequestToSendTime.clear();
		}
		this.monitorCurrentRequestsDuration = monitor;
	}

	@JmxAttribute(description = "shows duration of current requests " +
			"in case when monitorCurrentRequestsDuration == true")
	public List<String> getCurrentRequestsDuration() {
		SortedSet<UrlWithDuration> durations = new TreeSet<>();
		for (HttpClientConnection conn : currentRequestToSendTime.keySet()) {
			UrlWithTimestamp urlWithTimestamp = currentRequestToSendTime.get(conn);
			int duration = (int) (eventloop.currentTimeMillis() - urlWithTimestamp.getTimestamp());
			String url = urlWithTimestamp.getUrl();
			durations.add(new UrlWithDuration(url, duration));
		}

		List<String> formattedDurations = new ArrayList<>(durations.size());
		formattedDurations.add("Duration       Url");
		for (UrlWithDuration urlWithDuration : durations) {
			String url = urlWithDuration.getUrl();
			String duration = formatDuration(urlWithDuration.getDuration());
			String line = String.format("%s   %s", duration, url);
			formattedDurations.add(line);
		}
		return formattedDurations;
	}

	private static final class UrlWithTimestamp {
		private final String url;
		private final long timestamp;

		public UrlWithTimestamp(String url, long timestamp) {
			this.url = url;
			this.timestamp = timestamp;
		}

		public String getUrl() {
			return url;
		}

		public long getTimestamp() {
			return timestamp;
		}
	}

	private static final class UrlWithDuration implements Comparable<UrlWithDuration> {
		private final String url;
		private final int duration;

		public UrlWithDuration(String url, int duration) {
			this.url = url;
			this.duration = duration;
		}

		public String getUrl() {
			return url;
		}

		public int getDuration() {
			return duration;
		}

		@Override
		public int compareTo(UrlWithDuration other) {
			return -Integer.compare(duration, other.duration);
		}
	}

	private final class MonitorRequestDurationCallback implements ResultCallback<HttpResponse> {
		private final ResultCallback<HttpResponse> actualCallback;
		private final HttpClientConnection connection;

		public MonitorRequestDurationCallback(ResultCallback<HttpResponse> actualCallback,
		                                      HttpClientConnection connection) {
			this.actualCallback = actualCallback;
			this.connection = connection;
		}

		@Override
		public void onResult(HttpResponse result) {
			currentRequestToSendTime.remove(connection);
			actualCallback.onResult(result);
		}

		@Override
		public void onException(Exception exception) {
			currentRequestToSendTime.remove(connection);
			actualCallback.onException(exception);
		}
	}
}
