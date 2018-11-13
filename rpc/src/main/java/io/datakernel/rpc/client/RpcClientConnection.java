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

package io.datakernel.rpc.client;

import io.datakernel.async.Callback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.jmx.JmxRefreshable;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.protocol.*;
import io.datakernel.rpc.protocol.RpcStream.Listener;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static io.datakernel.rpc.client.IRpcClient.RPC_OVERLOAD_EXCEPTION;
import static io.datakernel.rpc.client.IRpcClient.RPC_TIMEOUT_EXCEPTION;
import static io.datakernel.util.Preconditions.checkArgument;
import static org.slf4j.LoggerFactory.getLogger;

public final class RpcClientConnection implements Listener, RpcSender, JmxRefreshable {
	private static final Logger logger = getLogger(RpcClientConnection.class);

	public static final RpcException CONNECTION_CLOSED = new RpcException(RpcClientConnection.class, "Connection closed.");
	public static final Duration DEFAULT_TIMEOUT_PRECISION = Duration.ofMillis(10);

	private final class TimeoutCookie implements Comparable<TimeoutCookie> {
		private final long timestamp;
		private final int cookie;

		public TimeoutCookie(int cookie, int timeout) {
			this.timestamp = eventloop.currentTimeMillis() + timeout;
			this.cookie = cookie;
		}

		public boolean isExpired() {
			return timestamp < eventloop.currentTimeMillis();
		}

		public int getCookie() {
			return cookie;
		}

		@Override
		public int compareTo(TimeoutCookie o) {
			return Long.compare(timestamp, o.timestamp);
		}
	}

	private final Eventloop eventloop;
	private final RpcClient rpcClient;
	private final RpcStream stream;
	private final InetSocketAddress address;
	private final Map<Integer, Callback<?>> activeRequests = new HashMap<>();
	private final PriorityQueue<TimeoutCookie> timeoutCookies = new PriorityQueue<>();
	private final Runnable expiredResponsesTask = createExpiredResponsesTask();

	private ScheduledRunnable scheduleExpiredResponsesTask;
	private int cookie = 0;
	private Duration timeoutPrecision = DEFAULT_TIMEOUT_PRECISION;
	private boolean connectionClosing;
	private boolean serverClosing;

	// JMX
	private boolean monitoring;
	private final RpcRequestStats connectionStats;
	private final EventStats totalRequests;
	private final EventStats connectionRequests;

	RpcClientConnection(Eventloop eventloop, RpcClient rpcClient,
			InetSocketAddress address,
			RpcStream stream) {
		this.eventloop = eventloop;
		this.rpcClient = rpcClient;
		this.stream = stream;
		this.address = address;

		// JMX
		this.monitoring = false;
		this.connectionStats = RpcRequestStats.create(RpcClient.SMOOTHING_WINDOW);
		this.connectionRequests = connectionStats.getTotalRequests();
		this.totalRequests = rpcClient.getGeneralRequestsStats().getTotalRequests();
	}

	public RpcClientConnection withTimeoutPrecision(Duration timeoutPrecision) {
		checkArgument(timeoutPrecision.toMillis() > 0, "Timeout precision cannot be zero or less");
		this.timeoutPrecision = timeoutPrecision;
		return this;
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, Callback<O> callback) {
		assert eventloop.inEventloopThread();

		// jmx
		totalRequests.recordEvent();
		connectionRequests.recordEvent();

		if (stream.isOverloaded() && !(request instanceof RpcMandatoryData)) {
			// jmx
			rpcClient.getGeneralRequestsStats().getRejectedRequests().recordEvent();
			connectionStats.getRejectedRequests().recordEvent();

			if (logger.isTraceEnabled()) logger.trace("RPC client uplink is overloaded");

			returnProtocolError(callback, RPC_OVERLOAD_EXCEPTION);
			return;
		}

		sendMessageData(request, timeout, callback);
	}

	private void sendMessageData(Object request, int timeout, Callback<?> callback) {
		cookie++;

		Callback<?> requestCallback = callback;

		// jmx
		if (isMonitoring()) {
			RpcRequestStats requestStatsPerClass = rpcClient.ensureRequestStatsPerClass(request.getClass());
			requestStatsPerClass.getTotalRequests().recordEvent();
			requestCallback = new JmxConnectionMonitoringResultCallback<>(requestStatsPerClass, callback, timeout);
		}
		TimeoutCookie timeoutCookie = new TimeoutCookie(cookie, timeout);
		addTimeoutCookie(timeoutCookie);
		activeRequests.put(cookie, requestCallback);

		stream.sendMessage(RpcMessage.of(cookie, request));
	}

	private void addTimeoutCookie(TimeoutCookie timeoutCookie) {
		if (timeoutCookies.isEmpty())
			scheduleExpiredResponsesTask();
		timeoutCookies.add(timeoutCookie);
	}

	private void scheduleExpiredResponsesTask() {
		if (connectionClosing)
			return;
		scheduleExpiredResponsesTask = eventloop.delayBackground(timeoutPrecision, expiredResponsesTask);
	}

	private Runnable createExpiredResponsesTask() {
		return () -> {
			checkExpiredResponses();
			if (!timeoutCookies.isEmpty())
				scheduleExpiredResponsesTask();
		};
	}

	private void checkExpiredResponses() {
		while (!timeoutCookies.isEmpty()) {
			TimeoutCookie timeoutCookie = timeoutCookies.peek();
			if (timeoutCookie == null)
				break;
			if (!activeRequests.containsKey(timeoutCookie.getCookie())) {
				timeoutCookies.remove();
				continue;
			}
			if (!timeoutCookie.isExpired())
				break;
			timeoutCookies.remove();
			doTimeout(timeoutCookie);
		}
	}

	private void doTimeout(TimeoutCookie timeoutCookie) {
		Callback<?> callback = activeRequests.remove(timeoutCookie.getCookie());
		if (callback == null)
			return;

		if (serverClosing && activeRequests.size() == 0) close();

		// jmx
		connectionStats.getExpiredRequests().recordEvent();
		rpcClient.getGeneralRequestsStats().getExpiredRequests().recordEvent();

		returnTimeout(callback, RPC_TIMEOUT_EXCEPTION);
	}

	private void returnTimeout(Callback<?> callback, Exception exception) {
		returnError(callback, exception);
	}

	private void returnProtocolError(Callback<?> callback, Exception exception) {
		returnError(callback, exception);
	}

	private void returnError(Callback<?> callback, Exception exception) {
		if (callback != null) {
			callback.setException(exception);
		}
	}

	@Override
	public void accept(RpcMessage message) {
		if (message.getData().getClass() == RpcRemoteException.class) {
			processError(message);
		} else if (message.getData().getClass() == RpcControlMessage.class) {
			handleControlMessage((RpcControlMessage) message.getData());
		} else {
			processResponse(message);
		}
	}

	private void handleControlMessage(RpcControlMessage controlMessage) {
		if (controlMessage == RpcControlMessage.CLOSE) {
			handleServerCloseMessage();
		} else {
			throw new RuntimeException("Received unknown RpcControlMessage");
		}
	}

	private void handleServerCloseMessage() {
		rpcClient.removeConnection(address);
		serverClosing = true;
		if (activeRequests.size() == 0) {
			close();
		}
	}

	private void processError(RpcMessage message) {
		RpcRemoteException remoteException = (RpcRemoteException) message.getData();
		// jmx
		connectionStats.getFailedRequests().recordEvent();
		rpcClient.getGeneralRequestsStats().getFailedRequests().recordEvent();
		connectionStats.getServerExceptions().recordException(remoteException, null);
		rpcClient.getGeneralRequestsStats().getServerExceptions().recordException(remoteException, null);

		Callback<?> callback = activeRequests.remove(message.getCookie());
		if (callback == null) return;

		returnError(callback, remoteException);
	}

	private void processResponse(RpcMessage message) {
		@SuppressWarnings("unchecked")
		Callback<Object> callback = (Callback<Object>) activeRequests.remove(message.getCookie());
		if (callback == null) return;

		callback.set(message.getData());
		if (serverClosing && activeRequests.size() == 0) {
			close();
		}
	}

	private void finishClosing() {
		if (scheduleExpiredResponsesTask != null)
			scheduleExpiredResponsesTask.cancel();
		if (!activeRequests.isEmpty()) {
			closeNotify();
		}
		rpcClient.removeConnection(address);
	}

	@Override
	public void onClosedWithError(Throwable exception) {
		finishClosing();

		// jmx
		String causedAddress = "Server address: " + address.getAddress().toString();
		logger.error("Protocol error. " + causedAddress, exception);
		rpcClient.getLastProtocolError().recordException(exception, causedAddress);
	}

	@Override
	public void onReadEndOfStream() {
		finishClosing();
	}

	private void closeNotify() {
		for (Integer cookie : new HashSet<>(activeRequests.keySet())) {
			returnProtocolError(activeRequests.remove(cookie), CONNECTION_CLOSED);
		}
	}

	public void close() {
		connectionClosing = true;
		stream.sendEndOfStream();
	}

	@Override
	public String toString() {
		return "RpcClientConnection{address=" + address + '}';
	}

	// JMX

	public void startMonitoring() {
		monitoring = true;
	}

	public void stopMonitoring() {
		monitoring = false;
	}

	private boolean isMonitoring() {
		return monitoring;
	}

	@JmxAttribute(name = "")
	public RpcRequestStats getRequestStats() {
		return connectionStats;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActiveRequests() {
		return activeRequests.size();
	}

	@Override
	public void refresh(long timestamp) {
		connectionStats.refresh(timestamp);
	}

	private final class JmxConnectionMonitoringResultCallback<T> implements Callback<T> {
		private final Stopwatch stopwatch;
		private final Callback<T> callback;
		private final RpcRequestStats requestStatsPerClass;
		private final long dueTimestamp;

		public JmxConnectionMonitoringResultCallback(RpcRequestStats requestStatsPerClass, Callback<T> callback,
				long timeout) {
			this.stopwatch = Stopwatch.createStarted();
			this.callback = callback;
			this.requestStatsPerClass = requestStatsPerClass;
			this.dueTimestamp = eventloop.currentTimeMillis() + timeout;
		}

		@Override
		public void set(T result) {
			int responseTime = timeElapsed();
			connectionStats.getResponseTime().recordValue(responseTime);
			requestStatsPerClass.getResponseTime().recordValue(responseTime);
			rpcClient.getGeneralRequestsStats().getResponseTime().recordValue(responseTime);
			recordOverdue();
			callback.set(result);
		}

		@Override
		public void setException(Throwable exception) {
			if (exception instanceof RpcRemoteException) {
				int responseTime = timeElapsed();
				connectionStats.getFailedRequests().recordEvent();
				connectionStats.getResponseTime().recordValue(responseTime);
				connectionStats.getServerExceptions().recordException(exception, null);
				requestStatsPerClass.getFailedRequests().recordEvent();
				requestStatsPerClass.getResponseTime().recordValue(responseTime);
				rpcClient.getGeneralRequestsStats().getResponseTime().recordValue(responseTime);
				requestStatsPerClass.getServerExceptions().recordException(exception, null);
				recordOverdue();
			} else if (exception instanceof AsyncTimeoutException) {
				connectionStats.getExpiredRequests().recordEvent();
				requestStatsPerClass.getExpiredRequests().recordEvent();
			} else if (exception instanceof RpcOverloadException) {
				connectionStats.getRejectedRequests().recordEvent();
				requestStatsPerClass.getRejectedRequests().recordEvent();
			}
			callback.setException(exception);
		}

		private int timeElapsed() {
			return (int) stopwatch.elapsed(TimeUnit.MILLISECONDS);
		}

		private void recordOverdue() {
			int overdue = (int) (System.currentTimeMillis() - dueTimestamp);
			if (overdue > 0) {
				connectionStats.getOverdues().recordValue(overdue);
				requestStatsPerClass.getOverdues().recordValue(overdue);
				rpcClient.getGeneralRequestsStats().getOverdues().recordValue(overdue);
			}
		}
	}
}
