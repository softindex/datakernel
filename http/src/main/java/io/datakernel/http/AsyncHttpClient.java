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

@SuppressWarnings({"ThrowableInstanceNeverThrown", "WeakerAccess"})
public class AsyncHttpClient extends AbstractClient<AsyncHttpClient> implements EventloopService, EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(AsyncHttpClient.class);
	private static final long CHECK_PERIOD = 1000L;
	private static final long DEFAULT_MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private static final BindException BIND_EXCEPTION = new BindException();

	private static class AddressConnects {
		private int pending;
		private int active;
		private int cached;
		private int totalCreated;
		private int totalClosed;
		private int totalTimeouts;
		private int totalExceptions;
		private Exception lastException;

		int getTotal() {
			return pending + active + cached;
		}

		private void addPending() {
			++pending;
			++totalCreated;
		}

		private void removePending() {
			--pending;
		}

		private void pending2active() {
			--pending;
			++active;
		}

		private void active2cached() {
			--active;
			++cached;
		}

		private void cached2active() {
			--cached;
			++active;
		}

		private void removeActive() {
			--active;
			++totalClosed;
		}

		private void removeCached() {
			--cached;
			++totalClosed;
		}

		void reset() {
			totalCreated = totalClosed = totalTimeouts = totalExceptions = 0;
			lastException = null;
		}

		@Override
		public String toString() {
			return "AddressConnects{" +
					"pending=" + pending +
					", active=" + active +
					", cached=" + cached +
					", totalCreated=" + totalCreated +
					", totalClosed=" + totalClosed +
					", totalTimeouts=" + totalTimeouts +
					", totalExceptions=" + totalExceptions +
					", lastException=" + lastException +
					'}';
		}
	}

	static class AddressConnectsMonitor {
		Map<InetSocketAddress, AddressConnects> addressConnects = new HashMap<>();
		int pending;
		int active;
		int cached;
		long totalCreated;
		long totalClosed;
		long totalTimeouts;
		long totalExceptions;

		void addPending(InetSocketAddress address) {
			AddressConnects connects = addressConnects.get(address);
			if (connects == null) {
				connects = new AddressConnects();
				addressConnects.put(address, connects);
			}
			connects.addPending();
			++pending;
			++totalCreated;
		}

		void removePending(InetSocketAddress address) {
			AddressConnects connects = addressConnects.get(address);
			connects.removePending();
			--pending;
			checkRemove(address, connects);
		}

		void pending2active(InetSocketAddress address) {
			addressConnects.get(address).pending2active();
			--pending;
			++active;
		}

		void active2cached(InetSocketAddress address) {
			addressConnects.get(address).active2cached();
			--active;
			++cached;
		}

		void cached2active(InetSocketAddress address) {
			addressConnects.get(address).cached2active();
			--cached;
			++active;
		}

		void removeActive(InetSocketAddress address) {
			AddressConnects connects = addressConnects.get(address);
			connects.removeActive();
			--active;
			++totalClosed;
			checkRemove(address, connects);
		}

		void removeCached(InetSocketAddress address) {
			AddressConnects connects = addressConnects.get(address);
			connects.removeCached();
			--cached;
			++totalClosed;
			checkRemove(address, connects);
		}

		void onException(InetSocketAddress address, Exception e) {
			AddressConnects connects = addressConnects.get(address);
			if (e instanceof TimeoutException) {
				++totalTimeouts;
				if (connects != null) {
					connects.totalTimeouts++;
				}
			} else {
				++totalExceptions;
				if (connects != null) {
					connects.totalExceptions++;
					connects.lastException = e;
				}
			}
		}

		private void checkRemove(InetSocketAddress address, AddressConnects connects) {
			if (connects.getTotal() == 0) {
				addressConnects.remove(address);
			}
		}

		void reset() {
			totalCreated = totalClosed = totalTimeouts = totalExceptions = 0;
			for (AddressConnects connects : addressConnects.values()) {
				connects.reset();
			}
		}
	}

	private static class BlockedAddress {
		final long timestamp;
		final Exception exception;
		final String info;

		BlockedAddress(long timestamp, Exception exception, String info) {
			this.timestamp = timestamp;
			this.exception = exception;
			this.info = info;
		}
	}

	private final DnsClient dnsClient;
	protected final ArrayList<AbstractHttpConnection> aliveConnections;
	protected final HashMap<InetSocketAddress, ExposedLinkedList<AbstractHttpConnection>> cachedAddressConnections = new HashMap<>();
	private final Map<InetSocketAddress, BlockedAddress> bindExceptionBlockedAddresses = new HashMap<>();
	private final Runnable expiredConnectionsTask = createExpiredConnectionsTask();
	private final char[] headerChars;

	AsyncCancellable scheduleExpiredConnectionCheck;
	private boolean blockLocalAddresses = false;
	private long bindExceptionBlockTimeout = 24 * 60 * 60 * 1000L;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	// timeouts
	private long maxIdleConnectionTime = DEFAULT_MAX_IDLE_CONNECTION_TIME;

	final AddressConnectsMonitor connectsMonitor = new AddressConnectsMonitor();
	private int inetAddressIdx = 0;

	// JMX
	private boolean monitoring;
	private final ValueStats timeCheckExpired = new ValueStats();
	private final ValueStats expiredConnections = new ValueStats();
	private final ExceptionStats dnsErrors = new ExceptionStats();

	// creators and builder methods
	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient) {
		this(eventloop, dnsClient, defaultSocketSettings());
	}

	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient, SocketSettings socketSettings) {
		super(eventloop, socketSettings);
		this.dnsClient = dnsClient;
		this.aliveConnections = new ArrayList<>();
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

	// timeouts
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

	int count;

	private int checkExpiredConnections() {
		scheduleExpiredConnectionCheck = null;
		Stopwatch stopwatch = (monitoring) ? Stopwatch.createStarted() : null;
		int count = 0;
		try {
			final long now = eventloop.currentTimeMillis();
			for (ExposedLinkedList<AbstractHttpConnection> pool : cachedAddressConnections.values()) {
				ExposedLinkedList.Node<AbstractHttpConnection> node = pool.getFirstNode();
				while (node != null) {
					AbstractHttpConnection connection = node.getValue();
					node = node.getNext();

					assert eventloop.inEventloopThread();
					long idleTime = now - connection.getLastUsedTime();
					if (idleTime < maxIdleConnectionTime)
						break; // connections must back ordered by activity
					aliveConnections.remove(connection);
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

	// core
	public void send(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		assert eventloop.inEventloopThread();
		checkNotNull(request);
		getUrlAsync(request, timeout, callback);
	}

	private HttpClientConnection getFreeConnection(HttpRequest request, InetSocketAddress address) {
		ExposedLinkedList<AbstractHttpConnection> list = cachedAddressConnections.get(address);
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
		final InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		if (isBindExceptionBlocked(address)) {
			callback.onException(BIND_EXCEPTION);
			return;
		}

		final long timeoutTime = eventloop.currentTimeMillis() + timeout;

		HttpClientConnection connection = getFreeConnection(request, address);
		if (connection != null) {
			connectsMonitor.cached2active(address);
			sendRequest(connection, request, timeoutTime, callback);
			return;
		}

		connectsMonitor.addPending(address);
		connect(address, timeout, isSecure(request), new SpecialConnectCallback() {
			@Override
			public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocket conn) {
				connectsMonitor.pending2active(address);

				ExposedLinkedList<AbstractHttpConnection> pool = ensureConnectionPool(address);

				HttpClientConnection connection = new HttpClientConnection(eventloop, address, conn, AsyncHttpClient.this, pool, headerChars, maxHttpMessageSize);
				aliveConnections.add(connection);
				sendRequest(connection, request, timeoutTime, callback);
				return connection;
			}

			@Override
			public void onException(Exception e) {
				if (logger.isWarnEnabled()) {
					logger.warn("Connect error to {} : {}", address, e.getMessage());
				}
				if (e instanceof BindException) {
					blockBindException(address, e);
				}
				connectsMonitor.removePending(address);
				callback.onException(e);
			}

			@Override
			public String toString() {
				return address.toString();
			}
		});
	}

	public ExposedLinkedList<AbstractHttpConnection> ensureConnectionPool(InetSocketAddress address) {
		ExposedLinkedList<AbstractHttpConnection> pool = cachedAddressConnections.get(address);
		if (pool == null) {
			pool = new ExposedLinkedList<>();
			cachedAddressConnections.put(address, pool);
		}
		return pool;
	}

	private boolean isSecure(HttpRequest request) {
		return isHttpsRequest(request);
	}

	private void sendRequest(final HttpClientConnection connection, HttpRequest request, long timeoutTime, final ResultCallback<HttpResponse> callback) {
		connection.send(request, timeoutTime, callback);
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

	private boolean isBindExceptionBlocked(InetSocketAddress inetAddress) {
		if (bindExceptionBlockTimeout == 0L)
			return false;
		BlockedAddress blocked = bindExceptionBlockedAddresses.get(inetAddress);
		if (blocked == null)
			return false;
		if (eventloop.currentTimeMillis() < blocked.timestamp + bindExceptionBlockTimeout)
			return true;
		bindExceptionBlockedAddresses.remove(inetAddress);
		return false;
	}

	private void blockBindException(InetSocketAddress address, Exception exception) {
		if (bindExceptionBlockTimeout == 0)
			return;
		AddressConnects addressConnects = connectsMonitor.addressConnects.get(address);
		String info = addressConnects.toString();
		BlockedAddress blocked = new BlockedAddress(eventloop.currentTimeMillis(), exception, info);
		bindExceptionBlockedAddresses.put(address, blocked);
		if (logger.isErrorEnabled()) {
			logger.error("BindException on " + address + ". " + info, exception);
		}
	}

	public void close() {
		checkState(eventloop.inEventloopThread());
		if (scheduleExpiredConnectionCheck != null)
			scheduleExpiredConnectionCheck.cancel();

		assert eventloop.inEventloopThread();
		for (Iterator<AbstractHttpConnection> it = aliveConnections.iterator(); it.hasNext(); ) {
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

	@JmxOperation
	public void resetStats() {
		timeCheckExpired.resetStats();
		connectsMonitor.reset();
	}

	@JmxAttribute
	public ValueStats getTimeCheckExpiredMicros() {
		return timeCheckExpired;
	}

	@JmxAttribute
	public String getTimeCheckExpiredMicrosStats() {
		return timeCheckExpired.toString();
	}

	@JmxAttribute
	public int getConnectionsCount() {
		return aliveConnections.size();
	}

	@JmxAttribute
	public List<String> fetchConnections() {
		List<String> info = new ArrayList<>();
		info.add("Address,keepAlive,LastUsedTime");
		for (AbstractHttpConnection connection : aliveConnections) {
			String string =
					StringUtils.join(',', new Object[]{
							connection.asyncTcpSocket.getRemoteSocketAddress(),
							connection.keepAlive,
							MBeanFormat.formatPeriodAgo(connection.getLastUsedTime())});
			info.add(string);
		}
		return info;
	}

	@JmxAttribute
	public List<String> fetchCachedAddressConnections() {
		if (cachedAddressConnections.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("Address,ConnectionsCount");
		for (Entry<InetSocketAddress, ExposedLinkedList<AbstractHttpConnection>> entry : cachedAddressConnections.entrySet()) {
			InetSocketAddress address = entry.getKey();
			ExposedLinkedList<AbstractHttpConnection> connections = entry.getValue();
			if (!connections.isEmpty()) {
				result.add(address + "," + connections.size());
			}
		}
		return result;
	}

	@JmxAttribute
	public ValueStats getExpiredConnectionsStats() {
		return expiredConnections;
	}

	@JmxAttribute
	public int getPendingConnectsCount() {
		return connectsMonitor.pending;
	}

	@JmxAttribute
	public int getActiveConnectionsCount() {
		return connectsMonitor.active;
	}

	@JmxAttribute
	public int getCachedConnectionsCount() {
		return connectsMonitor.cached;
	}

	@JmxAttribute
	public long getTotalConnectionsCreated() {
		return connectsMonitor.totalCreated;
	}

	@JmxAttribute
	public long getTotalConnectionsClosed() {
		return connectsMonitor.totalClosed;
	}

	@JmxAttribute
	public long getTotalConnectionsExceptions() {
		return connectsMonitor.totalExceptions;
	}

	@JmxAttribute
	public long getTotalConnectionsTimeouts() {
		return connectsMonitor.totalTimeouts;
	}

	@JmxAttribute
	public ExceptionStats getDnsErrors() {
		return dnsErrors;
	}

	@JmxAttribute
	public List<String> fetchAddressConnections() {
		if (connectsMonitor.addressConnects.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("Address,Pending,Active,Cached,Total,TotalCreated,TotalClosed,TotalTimeouts,TotalExceptions,LastException");
		for (Entry<InetSocketAddress, AddressConnects> entry : connectsMonitor.addressConnects.entrySet()) {
			AddressConnects data = entry.getValue();
			String exception = data.lastException == null ? "" : data.lastException.toString();
			result.add(
					StringUtils.join(',', new Object[]{
							entry.getKey(),
							data.pending,
							data.active,
							data.cached,
							data.getTotal(),
							data.totalCreated,
							data.totalClosed,
							data.totalTimeouts,
							data.totalExceptions, exception}));
		}
		return result;
	}

	@JmxAttribute
	public String fetchAddressConnection(String host, int port) {
		if (port == 0) {
			port = 80;
		}
		InetSocketAddress address = new InetSocketAddress(host, port);
		AddressConnects addressConnects = connectsMonitor.addressConnects.get(address);
		return addressConnects == null ? null : addressConnects.toString();
	}

	@JmxAttribute
	public List<String> getBindExceptionBlockedAddresses() {
		if (bindExceptionBlockedAddresses.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		result.add("Address,DateTime,Exception,Info");
		for (Entry<InetSocketAddress, BlockedAddress> entry : bindExceptionBlockedAddresses.entrySet()) {
			BlockedAddress blocked = entry.getValue();
			result.add(
					StringUtils.join(',', new Object[]{
							entry.getKey(),
							MBeanFormat.formatDateTime(blocked.timestamp),
							blocked.exception,
							blocked.info}));
		}
		return result;
	}
	//endregion
}
