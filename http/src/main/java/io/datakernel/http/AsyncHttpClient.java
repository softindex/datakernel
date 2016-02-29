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
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.ExposedLinkedList.Node;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.Joiner;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public class AsyncHttpClient implements EventloopService, ConcurrentJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(AsyncHttpClient.class);
	private static final long CHECK_PERIOD = 1000L;
	private static final long MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();
	private static final BindException BIND_EXCEPTION = new BindException();

	private final Eventloop eventloop;
	private final DnsClient dnsClient;
	private final SocketSettings socketSettings;
	protected final ExposedLinkedList<AbstractHttpConnection> connectionsList;
	private final Runnable expiredConnectionsTask = createExpiredConnectionsTask();
	private final HashMap<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> ipConnectionLists = new HashMap<>();
	private final HashMap<InetSocketAddress, Integer> addressPendingConnects = new HashMap<>();
	private final Map<InetAddress, Long> bindExceptionBlockedHosts = new HashMap<>();
	private final char[] headerChars;

	private AsyncCancellable scheduleExpiredConnectionCheck;
	private boolean blockLocalAddresses = false;
	private long bindExceptionBlockTimeout = 24 * 60 * 60 * 1000L;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	private boolean running;

	// JMX
	private final ValueStats timeCheckExpired = new ValueStats();
	private final ValueStats expiredConnections = new ValueStats();

	private final EventStats totalRequests = new EventStats();
	private final ExceptionStats dnsErrors = new ExceptionStats();

	private CountStats pendingSocketConnect = new CountStats();

	private boolean monitoring;

	private int inetAddressIdx = 0;

	/**
	 * Creates a new instance of HttpClientImpl with default socket settings
	 *
	 * @param eventloop eventloop in which will handle this connection
	 * @param dnsClient DNS client for resolving IP addresses for the specified host names
	 */
	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient) {
		this(eventloop, dnsClient, defaultSocketSettings());
	}

	/**
	 * Creates a new instance of HttpClientImpl
	 *
	 * @param eventloop      eventloop in which will handle this connection
	 * @param dnsClient      DNS client for resolving IP addresses for the specified host names
	 * @param socketSettings settings for creating socket
	 */
	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient, SocketSettings socketSettings) {
		this.eventloop = eventloop;
		this.dnsClient = dnsClient;
		this.socketSettings = checkNotNull(socketSettings);
		this.connectionsList = new ExposedLinkedList<>();
		char[] chars = eventloop.get(char[].class);
		if (chars == null || chars.length < MAX_HEADER_LINE_SIZE) {
			chars = new char[MAX_HEADER_LINE_SIZE];
			eventloop.set(char[].class, chars);
		}
		this.headerChars = chars;
	}

	public AsyncHttpClient setBindExceptionBlockTimeout(long bindExceptionBlockTimeout) {
		this.bindExceptionBlockTimeout = bindExceptionBlockTimeout;
		return this;
	}

	public AsyncHttpClient setBlockLocalAddresses(boolean blockLocalAddresses) {
		this.blockLocalAddresses = blockLocalAddresses;
		return this;
	}

	public AsyncHttpClient setMaxHttpMessageSize(int size) {
		this.maxHttpMessageSize = size;
		return this;
	}

	private Runnable createExpiredConnectionsTask() {
		return new Runnable() {
			@Override
			public void run() {
				checkExpiredConnections();
				if (!connectionsList.isEmpty())
					scheduleCheck();
			}
		};
	}

	private void scheduleCheck() {
		scheduleExpiredConnectionCheck = eventloop.schedule(eventloop.currentTimeMillis() + CHECK_PERIOD, expiredConnectionsTask);
	}

	private int checkExpiredConnections() {
		scheduleExpiredConnectionCheck = null;
		Stopwatch stopwatch = (monitoring) ? Stopwatch.createStarted() : null;
		int count = 0;
		try {
			final long now = eventloop.currentTimeMillis();

			ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
			while (node != null) {
				AbstractHttpConnection connection = node.getValue();
				node = node.getNext();

				assert connection.getEventloop().inEventloopThread();
				long idleTime = now - connection.getActivityTime();
				if (idleTime < MAX_IDLE_CONNECTION_TIME)
					break; // connections must back ordered by activity
				connection.close();
				count++;
			}
			expiredConnections.recordValue(count);
		} finally {
			if (stopwatch != null)
				timeCheckExpired.recordValue((int) stopwatch.elapsed(TimeUnit.MICROSECONDS));
		}
		return count;
	}

	private HttpClientConnection createConnection(SocketChannel socketChannel) {
		HttpClientConnection connection = new HttpClientConnection(eventloop, socketChannel, this, headerChars, maxHttpMessageSize);
		if (connectionsList.isEmpty())
			scheduleCheck();
		return connection;
	}

	private HttpClientConnection getFreeConnection(InetSocketAddress address) {
		ExposedLinkedList<HttpClientConnection> list = ipConnectionLists.get(address);
		if (list == null)
			return null;
		for (; ; ) {
			HttpClientConnection connection = list.removeFirstValue();
			if (connection == null)
				break;
			if (connection.isRegistered()) {
				connection.ipConnectionListNode = null;
				return connection;
			}
		}
		return null;
	}

	/**
	 * Puts the client connection to connections cache
	 *
	 * @param connection connections for putting
	 */
	protected void addToIpPool(HttpClientConnection connection) {
		assert connection.isRegistered();
		assert connection.ipConnectionListNode == null;

		InetSocketAddress address = connection.getRemoteSocketAddress();
		if (address == null) {
			connection.close();
			return;
		}
		ExposedLinkedList<HttpClientConnection> list = ipConnectionLists.get(address);
		if (list == null) {
			list = new ExposedLinkedList<>();
			ipConnectionLists.put(address, list);
		}
		connection.ipConnectionListNode = list.addLastValue(connection);
	}

	/**
	 * Removes the client's connection from connections cache
	 *
	 * @param connection connection for removing
	 */
	protected void removeFromIpPool(HttpClientConnection connection) {
		if (connection.ipConnectionListNode == null)
			return;
		InetSocketAddress address = connection.getRemoteSocketAddress();
		ExposedLinkedList<HttpClientConnection> list = ipConnectionLists.get(address);
		if (list != null) {
			list.removeNode(connection.ipConnectionListNode);
			connection.ipConnectionListNode = null;
			if (list.isEmpty()) {
				ipConnectionLists.remove(address);
			}
		}
	}

	/**
	 * Sends the request to server, waits the result timeout and handles result with callback
	 *
	 * @param request  request for server
	 * @param timeout  time which client will wait result
	 * @param callback callback for handling result
	 */
	public void execute(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		checkNotNull(request);
		assert eventloop.inEventloopThread();

		logger.trace("Calling {}", request);
		totalRequests.recordEvent();
		getUrlAsync(request, timeout, callback);
	}

	private void getUrlAsync(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		dnsClient.resolve4(request.getUrl().getHost(), new ResultCallback<InetAddress[]>() {
			@Override
			public void onResult(InetAddress[] inetAddresses) {
				logger.trace("dnsClient.resolve4.onResult Calling {}", request);
				getUrlForHostAsync(request, timeout, inetAddresses, callback);
			}

			@Override
			public void onException(Exception exception) {
				logger.trace("dnsClient.resolve4.onException Calling {}", request);
				if (exception.getClass() == DnsException.class || exception.getClass() == TimeoutException.class) {
					if (logger.isWarnEnabled()) {
						logger.warn("Unexpected DNS exception for '{}': {}", request, exception.getMessage());
					}
				} else {
					if (logger.isErrorEnabled()) {
						logger.error("Unexpected DNS exception for " + request, exception);
					}
				}
				dnsErrors.recordException(exception, request, eventloop.currentTimeMillis());
				callback.onException(exception);
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

		HttpClientConnection connection = getFreeConnection(address);
		if (connection != null) {
			sendRequest(connection, request, timeoutTime, callback);
			return;
		}

		logger.trace("eventloop.connect Calling {}", request);
		eventloop.connect(address, socketSettings, timeout, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				logger.trace("eventloop.connect.onConnect Calling {}", request);
				removePendingSocketConnect(address);
				HttpClientConnection connection = createConnection(socketChannel);
				connection.register();
				if (timeoutTime <= eventloop.currentTimeMillis()) {
					// timeout for this request, reuse for other requests
					addToIpPool(connection);
					callback.onException(TIMEOUT_EXCEPTION);
					return;
				}
				sendRequest(connection, request, timeoutTime, callback);
			}

			@Override
			public void onException(Exception exception) {
				logger.trace("eventloop.connect.onException Calling {}", request);
				removePendingSocketConnect(address);
				if (exception instanceof BindException) {
					if (bindExceptionBlockTimeout != 0) {
						bindExceptionBlockedHosts.put(inetAddress, eventloop.currentTimeMillis());
					}
				}

				if (logger.isWarnEnabled()) {
					logger.warn("Connect error to {} : {}", address, exception.getMessage());
				}
				callback.onException(exception);
			}

			@Override
			public String toString() {
				return address.toString();
			}
		});
		addPendingSocketConnect(address);
	}

	private void sendRequest(final HttpClientConnection connection, HttpRequest request, long timeoutTime, final ResultCallback<HttpResponse> callback) {
		connectionsList.moveNodeToLast(connection.connectionsListNode); // back-order connections
		logger.trace("sendRequest Calling {}", request);
		connection.request(request, timeoutTime, callback);
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

	/**
	 * Closes this client's handler without callback
	 */
	public void close() {
		checkState(eventloop.inEventloopThread());
		if (scheduleExpiredConnectionCheck != null)
			scheduleExpiredConnectionCheck.cancel();

		ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert connection.getEventloop().inEventloopThread();
			connection.close();
		}
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	/**
	 * Starts this client's handler and handles completing of this action with callback
	 *
	 * @param callback for handling completing of start
	 */
	@Override
	public void start(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		checkState(!running);
		running = true;
		callback.onComplete();
	}

	/**
	 * Stops this clients handler ,close all connections and handles completing of this
	 * action with callback
	 *
	 * @param callback for handling completing of stop
	 */
	@Override
	public void stop(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		if (running) {
			running = false;
			close();
		}
		callback.onComplete();
	}

	public CompletionCallbackFuture closeFuture() {
		return AsyncCallbacks.stopFuture(this);
	}

	// JMX

//	public void resetStats() {
//		timeCheckExpired.resetStats();
//	}

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
		for (Entry<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> entry : ipConnectionLists.entrySet()) {
			InetSocketAddress address = entry.getKey();
			ExposedLinkedList<HttpClientConnection> connections = entry.getValue();
			result.add(address + "," + connections.size());
		}
		return result;
	}

	@JmxAttribute
	public int getConnectionsCount() {
		return connectionsList.size();
	}

	@JmxAttribute
	public List<String> getConnections() {
		Joiner joiner = Joiner.on(',');
		List<String> info = new ArrayList<>();
		info.add("RemoteSocketAddress,isRegistered,LifeTime,ActivityTime");
		for (Node<AbstractHttpConnection> node = connectionsList.getFirstNode(); node != null; node = node.getNext()) {
			AbstractHttpConnection connection = node.getValue();
			String string = joiner.join(connection.getRemoteSocketAddress(), connection.isRegistered(),
					MBeanFormat.formatPeriodAgo(connection.getLifeTime()),
					MBeanFormat.formatPeriodAgo(connection.getActivityTime())
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

	@Override
	public Executor getJmxExecutor() {
		return eventloop;
	}
}
