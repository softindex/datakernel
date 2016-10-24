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

package io.datakernel.rpc.client;

import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxRefreshable;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static org.slf4j.LoggerFactory.getLogger;

public final class RpcClientConnection implements RpcConnection, RpcSender, JmxRefreshable {
	public static final int DEFAULT_TIMEOUT_PRECISION = 10; //ms

	private final class TimeoutCookie implements Comparable<TimeoutCookie> {
		private final int timeout;
		private final long timestamp;
		private final int cookie;

		public TimeoutCookie(int cookie, int timeout) {
			this.timeout = timeout;
			this.timestamp = eventloop.currentTimeMillis() + timeout;
			this.cookie = cookie;
		}

		public int getTimeoutMillis() {
			return timeout;
		}

		public boolean isExpired() {
			return timestamp < eventloop.currentTimeMillis();
		}

		public int getCookie() {
			return cookie;
		}

		public int getElapsedTime() {
			return (int) (eventloop.currentTimeMillis() - timestamp + timeout);
		}

		@Override
		public int compareTo(TimeoutCookie o) {
			return Long.compare(timestamp, o.timestamp);
		}
	}

	private final Logger logger = getLogger(this.getClass());
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final RpcOverloadException OVERLOAD_EXCEPTION =
			new RpcOverloadException("Write connection is overloaded");
	private final Eventloop eventloop;
	private final RpcClient rpcClient;
	private final RpcProtocol protocol;
	private final InetSocketAddress address;
	private final Map<Integer, ResultCallback<?>> requests = new HashMap<>();
	private final PriorityQueue<TimeoutCookie> timeoutCookies = new PriorityQueue<>();
	private final Runnable expiredResponsesTask = createExpiredResponsesTask();

	private AsyncCancellable scheduleExpiredResponsesTask;
	private int cookieCounter = 0;
	private boolean connectionClosing;
	private boolean serverClosing;

	// JMX
	private boolean monitoring;
	private RpcRequestStats requestsStats;

	private RpcClientConnection(Eventloop eventloop, RpcClient rpcClient,
	                            AsyncTcpSocket asyncTcpSocket, InetSocketAddress address,
	                            BufferSerializer<RpcMessage> messageSerializer,
	                            RpcProtocolFactory protocolFactory) {
		this.eventloop = eventloop;
		this.rpcClient = rpcClient;
		this.protocol = protocolFactory.createClientProtocol(eventloop, asyncTcpSocket, this, messageSerializer);
		this.address = address;

		// JMX
		this.monitoring = false;
		this.requestsStats = RpcRequestStats.create();
	}

	public static RpcClientConnection create(Eventloop eventloop, RpcClient rpcClient,
	                                         AsyncTcpSocket asyncTcpSocket, InetSocketAddress address,
	                                         BufferSerializer<RpcMessage> messageSerializer,
	                                         RpcProtocolFactory protocolFactory) {
		return new RpcClientConnection(eventloop, rpcClient, asyncTcpSocket, address,
				messageSerializer, protocolFactory);
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
		assert eventloop.inEventloopThread();

		// jmx
		requestsStats.getTotalRequests().recordEvent();
		rpcClient.getGeneralRequestsStats().getTotalRequests().recordEvent();

		if (!(request instanceof RpcMandatoryData) && protocol.isOverloaded()) {
			// jmx
			requestsStats.getRejectedRequests().recordEvent();
			rpcClient.getGeneralRequestsStats().getRejectedRequests().recordEvent();

			if (logger.isWarnEnabled())
				logger.warn(OVERLOAD_EXCEPTION.getMessage());
			returnProtocolError(callback, OVERLOAD_EXCEPTION);
			return;
		}
		sendMessageData(request, timeout, callback);
	}

	private void sendMessageData(Object request, int timeout, ResultCallback<?> callback) {
		cookieCounter++;
		if (requests.containsKey(cookieCounter)) {
			String msg = "Request ID " + cookieCounter + " is already in use";
			if (logger.isErrorEnabled())
				logger.error(msg);
			throw new IllegalStateException(msg);
		}

		ResultCallback<?> requestCallback = callback;

		// jmx
		if (isMonitoring()) {
			Class<?> requestClass = request.getClass();
			rpcClient.ensureRequestStatsPerClass(requestClass).getTotalRequests().recordEvent();
			requestCallback = new JmxConnectionMonitoringResultCallback<>(request.getClass(), callback);
		}

		TimeoutCookie timeoutCookie = new TimeoutCookie(cookieCounter, timeout);
		addTimeoutCookie(timeoutCookie);
		requests.put(cookieCounter, requestCallback);

		protocol.sendMessage(RpcMessage.of(cookieCounter, request));
	}

	private void addTimeoutCookie(TimeoutCookie timeoutCookie) {
		if (timeoutCookies.isEmpty())
			scheduleExpiredResponsesTask();
		timeoutCookies.add(timeoutCookie);
	}

	private void scheduleExpiredResponsesTask() {
		if (connectionClosing)
			return;
		scheduleExpiredResponsesTask = eventloop.schedule(eventloop.currentTimeMillis() + DEFAULT_TIMEOUT_PRECISION, expiredResponsesTask);
	}

	private Runnable createExpiredResponsesTask() {
		return new Runnable() {
			@Override
			public void run() {
				checkExpiredResponses();
				if (!timeoutCookies.isEmpty())
					scheduleExpiredResponsesTask();
			}
		};
	}

	private void checkExpiredResponses() {
		while (!timeoutCookies.isEmpty()) {
			TimeoutCookie timeoutCookie = timeoutCookies.peek();
			if (timeoutCookie == null)
				break;
			if (!requests.containsKey(timeoutCookie.getCookie())) {
				timeoutCookies.remove();
				continue;
			}
			if (!timeoutCookie.isExpired())
				break;
			doTimeout(timeoutCookie);
			timeoutCookies.remove();
		}
	}

	private void doTimeout(TimeoutCookie timeoutCookie) {
		ResultCallback<?> callback = requests.remove(timeoutCookie.getCookie());
		if (callback == null)
			return;

		// jmx
		requestsStats.getExpiredRequests().recordEvent();
		rpcClient.getGeneralRequestsStats().getExpiredRequests().recordEvent();

		returnTimeout(callback, new RpcTimeoutException("Timeout (" + timeoutCookie.getElapsedTime() + "/" + timeoutCookie.getTimeoutMillis()
				+ " ms) for server (" + address + ") response for request ID " + timeoutCookie.getCookie()));
	}

	private void removeTimeoutCookie(TimeoutCookie timeoutCookie) {
		timeoutCookies.remove(timeoutCookie);
	}

	private void returnTimeout(ResultCallback<?> callback, Exception exception) {
		returnError(callback, exception);
	}

	private void returnProtocolError(ResultCallback<?> callback, Exception exception) {
		returnError(callback, exception);
	}

	private void returnError(ResultCallback<?> callback, Exception exception) {
		if (callback != null) {
			callback.setException(exception);
		}
	}

	@Override
	public void onData(RpcMessage message) {
		if (message.getData().getClass() == RpcRemoteException.class) {
			RpcRemoteException remoteException = (RpcRemoteException) message.getData();

			// jmx
			requestsStats.getFailedRequests().recordEvent();
			rpcClient.getGeneralRequestsStats().getFailedRequests().recordEvent();
			requestsStats.getServerExceptions().recordException(remoteException, null);
			rpcClient.getGeneralRequestsStats().getServerExceptions().recordException(remoteException, null);

			processError(message, remoteException);
		} else if (message.getData().getClass() == RpcControlMessage.class) {
			handleControlMessage((RpcControlMessage) message.getData());
		} else {
			// jmx
			requestsStats.getSuccessfulRequests().recordEvent();
			rpcClient.getGeneralRequestsStats().getSuccessfulRequests().recordEvent();

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
		if (requests.size() == 0) {
			close();
		}
	}

	private void processError(RpcMessage message, RpcRemoteException exception) {
		ResultCallback<?> callback = removeResultCallback(message);
		if (callback == null)
			return;
		returnError(callback, exception);
	}

	private void processResponse(RpcMessage message) {
		ResultCallback<Object> callback = removeResultCallback(message);

		if (serverClosing) {
			if (requests.size() == 0) {
				close();
			}
		}

		if (callback == null)
			return;
		callback.setResult(message.getData());
	}

	@SuppressWarnings("unchecked")
	private <T> ResultCallback<T> removeResultCallback(RpcMessage message) {
		return (ResultCallback<T>) requests.remove(message.getCookie());
	}

	private void finishClosing() {
		if (scheduleExpiredResponsesTask != null)
			scheduleExpiredResponsesTask.cancel();
		if (!requests.isEmpty()) {
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

	@Override
	public AsyncTcpSocket.EventHandler getSocketConnection() {
		return protocol.getSocketConnection();
	}

	private void closeNotify() {
		for (Integer cookie : new HashSet<>(requests.keySet())) {
			returnProtocolError(requests.remove(cookie), new RpcException("Connection closed."));
		}
	}

	public void close() {
		connectionClosing = true;
		protocol.sendEndOfStream();
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

	public void resetStats() {
		requestsStats.resetStats();
	}

	@JmxAttribute(name = "")
	public RpcRequestStats getRequestStats() {
		return requestsStats;
	}

	@Override
	public void refresh(long timestamp) {
		requestsStats.refresh(timestamp);
	}

	private final class JmxConnectionMonitoringResultCallback<T> extends ResultCallback<T> {

		private final Stopwatch stopwatch;
		private final ResultCallback<T> callback;
		private final Class<?> requestClass;

		public JmxConnectionMonitoringResultCallback(Class<?> requestClass, ResultCallback<T> callback) {
			this.stopwatch = Stopwatch.createStarted();
			this.callback = callback;
			this.requestClass = requestClass;
		}

		@Override
		public void onResult(T result) {
			int responseTime = timeElapsed();
			requestsStats.getSuccessfulRequests().recordEvent();
			rpcClient.ensureRequestStatsPerClass(requestClass).getSuccessfulRequests().recordEvent();
			requestsStats.getResponseTime().recordValue(responseTime);
			rpcClient.ensureRequestStatsPerClass(requestClass).getResponseTime().recordValue(responseTime);
			rpcClient.getGeneralRequestsStats().getResponseTime().recordValue(responseTime);

			callback.setResult(result);
		}

		@Override
		public void onException(Exception exception) {
			if (exception instanceof RpcRemoteException) {
				int responseTime = timeElapsed();
				requestsStats.getFailedRequests().recordEvent();
				requestsStats.getResponseTime().recordValue(responseTime);
				requestsStats.getServerExceptions().recordException(exception, null);
				rpcClient.ensureRequestStatsPerClass(requestClass).getFailedRequests().recordEvent();
				rpcClient.ensureRequestStatsPerClass(requestClass).getResponseTime().recordValue(responseTime);
				rpcClient.getGeneralRequestsStats().getResponseTime().recordValue(responseTime);
				rpcClient.ensureRequestStatsPerClass(requestClass).getServerExceptions().recordException(exception, null);
			} else if (exception instanceof RpcTimeoutException) {
				requestsStats.getExpiredRequests().recordEvent();
				rpcClient.ensureRequestStatsPerClass(requestClass).getExpiredRequests().recordEvent();
			} else if (exception instanceof RpcOverloadException) {
				requestsStats.getRejectedRequests().recordEvent();
				rpcClient.ensureRequestStatsPerClass(requestClass).getRejectedRequests().recordEvent();
			}

			callback.setException(exception);
		}

		private int timeElapsed() {
			return (int) (stopwatch.elapsed(TimeUnit.MILLISECONDS));
		}
	}
}
