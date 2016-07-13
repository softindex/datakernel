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
import io.datakernel.dns.DnsException;
import io.datakernel.eventloop.AbstractClient;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.Stopwatch;
import io.datakernel.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;

@SuppressWarnings({"ThrowableInstanceNeverThrown", "WeakerAccess"})
public class AsyncHttpClient extends AbstractClient<AsyncHttpClient> implements EventloopService, EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(AsyncHttpClient.class);
	private static final long CHECK_PERIOD = 1000L;
	private static final long DEFAULT_MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private static final BindException BIND_EXCEPTION = new BindException();

	private final DnsClient dnsClient;
	protected final ArrayList<AbstractHttpConnection> activeConnections;
	protected final HashMap<InetSocketAddress, ExposedLinkedList<AbstractHttpConnection>> ipConnectionLists = new HashMap<>();
	private final Runnable expiredConnectionsTask = createExpiredConnectionsTask();
	private final HashMap<InetSocketAddress, Integer> addressPendingConnects = new HashMap<>();
	private final Map<InetAddress, Long> bindExceptionBlockedHosts = new HashMap<>();
	private final char[] headerChars;

	AsyncCancellable scheduleExpiredConnectionCheck;
	private boolean blockLocalAddresses = false;
	private long bindExceptionBlockTimeout = 24 * 60 * 60 * 1000L;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	// timeouts
	private long maxIdleConnectionTime = DEFAULT_MAX_IDLE_CONNECTION_TIME;

	// JMX
	private final ValueStats timeCheckExpired = new ValueStats();
	private final ValueStats expiredConnections = new ValueStats();

	private final EventStats totalRequests = new EventStats();
	private final ExceptionStats dnsErrors = new ExceptionStats();

	private CountStats pendingSocketConnect = new CountStats();

	private boolean monitoring;

	private int inetAddressIdx = 0;

	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient) {
		this(eventloop, dnsClient, defaultSocketSettings());
	}

	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient, SocketSettings socketSettings) {
		super(eventloop, socketSettings);
		this.dnsClient = dnsClient;
		this.activeConnections = new ArrayList<>();
		char[] chars = eventloop.get(char[].class);
		if (chars == null || chars.length < MAX_HEADER_LINE_SIZE) {
			chars = new char[MAX_HEADER_LINE_SIZE];
			eventloop.set(char[].class, chars);
		}
		this.headerChars = chars;
	}

	public AsyncHttpClient keepConnectionAlive(long time) {
		this.maxIdleConnectionTime = time;
		return this;
	}

	public AsyncHttpClient bindExceptionBlockTimeout(long bindExceptionBlockTimeout) {
		this.bindExceptionBlockTimeout = bindExceptionBlockTimeout;
		return this;
	}

	public AsyncHttpClient blockLocalAddresses(boolean blockLocalAddresses) {
		this.blockLocalAddresses = blockLocalAddresses;
		return this;
	}

	public AsyncHttpClient maxHttpMessageSize(int size) {
		this.maxHttpMessageSize = size;
		return this;
	}

	private Runnable createExpiredConnectionsTask() {
		return new Runnable() {
			@Override
			public void run() {
				checkExpiredConnections();
				scheduleCheck();
			}
		};
	}

	void scheduleCheck() {
		if (scheduleExpiredConnectionCheck == null) {
			scheduleExpiredConnectionCheck = eventloop.scheduleBackground(eventloop.currentTimeMillis() + CHECK_PERIOD, expiredConnectionsTask);
		}
	}

	private int checkExpiredConnections() {
		scheduleExpiredConnectionCheck = null;
		Stopwatch stopwatch = (monitoring) ? Stopwatch.createStarted() : null;
		int count = 0;
		try {
			final long now = eventloop.currentTimeMillis();
			for (ExposedLinkedList<AbstractHttpConnection> pool : ipConnectionLists.values()) {
				ExposedLinkedList.Node<AbstractHttpConnection> node = pool.getFirstNode();
				while (node != null) {
					AbstractHttpConnection connection = node.getValue();
					node = node.getNext();

					assert eventloop.inEventloopThread();
					long idleTime = now - connection.getLastUsedTime();
					if (idleTime < maxIdleConnectionTime)
						break; // connections must back ordered by activity
					activeConnections.remove(connection);
					connection.close();
					count++;
				}
				expiredConnections.recordValue(count);
			}
		} finally {
			if (stopwatch != null)
				timeCheckExpired.recordValue((int) stopwatch.elapsed(TimeUnit.MICROSECONDS));
		}
		return count;
	}

	private HttpClientConnection getFreeConnection(HttpRequest request, InetSocketAddress address) {
		ExposedLinkedList<AbstractHttpConnection> list = ipConnectionLists.get(address);
		if (list == null)
			return null;
		for (; ; ) {
			HttpClientConnection connection = (HttpClientConnection) list.removeFirstValue();
			if (connection == null)
				break;
			if (!connection.isClosed()) {
				if (isHttpsRequest(request)) {
					if (!connection.isSslConnection()) {
						return null;
					}
				} else {
					if (connection.isSslConnection()) {
						return null;
					}
				}
				connection.ipConnectionListNode = null;
				return connection;
			}
		}
		return null;
	}

	private boolean isHttpsRequest(HttpRequest request) {
		return request.getUrl().getSchema().equals("https");
	}

	public void send(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		checkNotNull(request);
		assert eventloop.inEventloopThread();

		totalRequests.recordEvent();
		getUrlAsync(request, timeout, callback);
	}

	private void getUrlAsync(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		dnsClient.resolve4(request.getUrl().getHost(), new ResultCallback<InetAddress[]>() {
			@Override
			public void onResult(InetAddress[] inetAddresses) {
				getUrlForHostAsync(request, timeout, inetAddresses, callback);
			}

			@Override
			public void onException(Exception e) {
				if (e.getClass() == DnsException.class || e.getClass() == TimeoutException.class) {
					if (logger.isWarnEnabled()) {
						logger.warn("DNS exception for '{}': {}", request, e.getMessage());
					}
				} else {
					if (logger.isErrorEnabled()) {
						logger.error("Unexpected DNS exception for " + request, e);
					}
				}
				dnsErrors.recordException(e, request);
				callback.onException(e);
			}
		});
	}

	private InetAddress getNextInetAddress(InetAddress[] inetAddresses) {
		return inetAddresses[((inetAddressIdx++) & Integer.MAX_VALUE) % inetAddresses.length];
	}

	private void getUrlForHostAsync(final HttpRequest request, int timeout, final InetAddress[] inetAddresses, final ResultCallback<HttpResponse> callback) {
		String host = request.getUrl().getHost();
		final InetAddress inetAddress = getNextInetAddress(inetAddresses);
		if (!isValidHost(host, inetAddress, blockLocalAddresses)) {
			callback.onException(new IOException("Invalid IP address " + inetAddress + " for host " + host));
			return;
		}
		if (isBindExceptionBlocked(inetAddress)) {
			callback.onException(BIND_EXCEPTION);
			return;
		}

		final long timeoutTime = eventloop.currentTimeMillis() + timeout;
		final InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		HttpClientConnection connection = getFreeConnection(request, address);
		if (connection != null) {
			sendRequest(connection, request, timeoutTime, callback);
			return;
		}

		connect(address, timeout, isSecure(request), new SpecialConnectCallback() {
			@Override
			public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocket conn) {
				removePendingSocketConnect(address);
				ExposedLinkedList<AbstractHttpConnection> pool = ipConnectionLists.get(address);
				if (pool == null) {
					pool = new ExposedLinkedList<>();
					ipConnectionLists.put(address, pool);
				}
				HttpClientConnection connection = new HttpClientConnection(eventloop, address, conn, AsyncHttpClient.this, pool, headerChars, maxHttpMessageSize);
				activeConnections.add(connection);
				sendRequest(connection, request, timeoutTime, callback);
				return connection;
			}

			@Override
			public void onException(Exception e) {
				logger.trace("eventloop.connect.onException Calling {}", request);
				removePendingSocketConnect(address);
				if (e instanceof BindException) {
					if (bindExceptionBlockTimeout != 0) {
						bindExceptionBlockedHosts.put(inetAddress, eventloop.currentTimeMillis());
					}
				}
				if (logger.isWarnEnabled()) {
					logger.warn("Connect error to {} : {}", address, e.getMessage());
				}
				callback.onException(e);
			}

			@Override
			public String toString() {
				return address.toString();
			}
		});

		addPendingSocketConnect(address);
	}

	private boolean isSecure(HttpRequest request) {
		return isHttpsRequest(request);
	}

	private void sendRequest(final HttpClientConnection connection, HttpRequest request, long timeoutTime, final ResultCallback<HttpResponse> callback) {
		connection.send(request, timeoutTime, callback);
	}

	private void addPendingSocketConnect(InetSocketAddress address) {
		// jmx
		pendingSocketConnect.increment();

		Integer counter = addressPendingConnects.get(address);
		if (counter == null) {
			counter = 0;
		}
		addressPendingConnects.put(address, ++counter);
	}

	private void removePendingSocketConnect(InetSocketAddress address) {
		// jmx
		pendingSocketConnect.decrement();

		Integer counter = addressPendingConnects.get(address);
		if (counter != null) {
			if (--counter > 0)
				addressPendingConnects.put(address, counter);
			else
				addressPendingConnects.remove(address);
		}
	}

	private static boolean isValidHost(String host, InetAddress inetAddress, boolean blockLocalAddresses) {
		byte[] addressBytes = inetAddress.getAddress();
		if (addressBytes == null || addressBytes.length != 4)
			return false;
		// 0.0.0.*
		if (addressBytes[0] == 0 && addressBytes[1] == 0 && addressBytes[2] == 0)
			return false;
		if (!blockLocalAddresses)
			return true;
		// 127.0.0.1
		if (addressBytes[0] == 127 && addressBytes[1] == 0 && addressBytes[2] == 0 && addressBytes[3] == 1)
			return false;
		if ("localhost".equals(host) || "127.0.0.1".equals(host))
			return false;
		return true;
	}

	private boolean isBindExceptionBlocked(InetAddress inetAddress) {
		if (bindExceptionBlockTimeout == 0L)
			return false;
		Long bindExceptionTimestamp = bindExceptionBlockedHosts.get(inetAddress);
		if (bindExceptionTimestamp == null)
			return false;
		if (eventloop.currentTimeMillis() < bindExceptionTimestamp + bindExceptionBlockTimeout)
			return true;
		bindExceptionBlockedHosts.remove(inetAddress);
		return false;
	}

	public void close() {
		checkState(eventloop.inEventloopThread());
		if (scheduleExpiredConnectionCheck != null)
			scheduleExpiredConnectionCheck.cancel();

		assert eventloop.inEventloopThread();
		for (Iterator<AbstractHttpConnection> it = activeConnections.iterator(); it.hasNext(); ) {
			AbstractHttpConnection conn = it.next();
			conn.close();
			it.remove();
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

	//region jmx
	@JmxOperation
	public void startMonitoring() {
		monitoring = true;
	}

	@JmxOperation
	public void stopMonitoring() {
		monitoring = false;
	}

	@JmxAttribute
	public EventStats getTotalRequests() {
		return totalRequests;
	}

	@JmxAttribute
	public ExceptionStats getDnsErrors() {
		return dnsErrors;
	}

	@JmxAttribute
	public ValueStats getTimeCheckExpiredMicros() {
		return timeCheckExpired;
	}

	@JmxAttribute
	public List<String> getAddressConnections() {
		if (ipConnectionLists.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("SocketAddress,ConnectionsCount");
		for (Entry<InetSocketAddress, ExposedLinkedList<AbstractHttpConnection>> entry : ipConnectionLists.entrySet()) {
			InetSocketAddress address = entry.getKey();
			ExposedLinkedList<AbstractHttpConnection> connections = entry.getValue();
			result.add(address + "," + connections.size());
		}
		return result;
	}

	@JmxAttribute
	public int getConnectionsCount() {
		return activeConnections.size();
	}

	@JmxAttribute
	public List<String> getConnections() {
		List<String> info = new ArrayList<>();
		info.add("RemoteSocketAddress,isRegistered,LifeTime,ActivityTime");
		for (AbstractHttpConnection activeConnection : activeConnections) {
			HttpClientConnection connection = (HttpClientConnection) activeConnection;
			String string = StringUtils.join(",",
					asList(
							connection.getRemoteSocketAddress(),
							!connection.isClosed(),
							MBeanFormat.formatPeriodAgo(connection.getLastUsedTime())
					)
			);
			info.add(string);
		}
		return info;
	}

	@JmxAttribute
	public ValueStats getExpiredConnectionsStats() {
		return expiredConnections;
	}

	@JmxAttribute
	public CountStats getPendingConnectsCount() {
		return pendingSocketConnect;
	}

	@JmxAttribute
	public List<String> getAddressPendingConnects() {
		if (addressPendingConnects.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("Address, Connects");
		for (Entry<InetSocketAddress, Integer> entry : addressPendingConnects.entrySet()) {
			result.add(entry.getKey() + ", " + entry.getValue());
		}
		return result;
	}

	@JmxAttribute
	public List<String> getBindExceptionBlockedHosts() {
		if (bindExceptionBlockedHosts.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("Address,DateTime");
		for (Entry<InetAddress, Long> entry : bindExceptionBlockedHosts.entrySet()) {
			result.add(entry.getKey() + "," + MBeanFormat.formatDateTime(entry.getValue()));
		}
		return result;
	}
	//endregion
}
