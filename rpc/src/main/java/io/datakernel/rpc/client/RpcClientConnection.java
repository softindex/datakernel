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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static org.slf4j.LoggerFactory.getLogger;

public final class RpcClientConnection implements RpcConnection, RpcSender {
	public static final int DEFAULT_TIMEOUT_PRECISION = 10; //ms

	interface StatusListener {
		void onOpen(RpcClientConnection connection);

		void onClosed();
	}

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

	private static final Logger logger = getLogger(RpcClientConnection.class);
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final RpcOverloadException OVERLOAD_EXCEPTION =
			new RpcOverloadException("Write connection is overloaded");
	private final Eventloop eventloop;
	private final RpcProtocol protocol;
	private final StatusListener statusListener;
	private final Map<Integer, ResultCallback<?>> requests = new HashMap<>();
	private final PriorityQueue<TimeoutCookie> timeoutCookies = new PriorityQueue<>();
	private final Runnable expiredResponsesTask = createExpiredResponsesTask();

	private AsyncCancellable scheduleExpiredResponsesTask;
	private int cookieCounter = 0;
	private boolean closing;

	// JMX
	private boolean monitoring;
	private RpcRequestStats requestsStats;

	public RpcClientConnection(Eventloop eventloop, SocketChannel socketChannel,
	                           BufferSerializer<RpcMessage> messageSerializer,
	                           RpcProtocolFactory protocolFactory, StatusListener statusListener) {
		this.eventloop = eventloop;
		this.statusListener = statusListener;
		this.protocol = protocolFactory.create(this, socketChannel, messageSerializer, false);

		// JMX
		this.monitoring = false;
		this.requestsStats = new RpcRequestStats();
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
		assert eventloop.inEventloopThread();

		// jmx
		requestsStats.getTotalRequests().recordEvent();

		if (!(request instanceof RpcMandatoryData) && protocol.isOverloaded()) {

			// jmx
			requestsStats.getRejectedRequests().recordEvent();

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
			returnProtocolError(callback, new IllegalStateException(msg));
			return;
		}

		ResultCallback<?> requestCallback = callback;

		// jmx
		if (isMonitoring()) {
			requestCallback = new JmxConnectionMonitoringResultCallback<>(callback);
		}

		TimeoutCookie timeoutCookie = new TimeoutCookie(cookieCounter, timeout);
		addTimeoutCookie(timeoutCookie);
		requests.put(cookieCounter, requestCallback);

		protocol.sendMessage(new RpcMessage(cookieCounter, request));
	}

	private void addTimeoutCookie(TimeoutCookie timeoutCookie) {
		if (timeoutCookies.isEmpty())
			scheduleExpiredResponsesTask();
		timeoutCookies.add(timeoutCookie);
	}

	private void scheduleExpiredResponsesTask() {
		if (closing)
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

		returnTimeout(callback, new RpcTimeoutException("Timeout (" + timeoutCookie.getElapsedTime() + "/" + timeoutCookie.getTimeoutMillis()
				+ " ms) for server response for request ID " + timeoutCookie.getCookie()));
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
			callback.onException(exception);
		}
	}

	@Override
	public void onReceiveMessage(RpcMessage message) {
		if (message.getData().getClass() == RpcRemoteException.class) {
			RpcRemoteException remoteException = (RpcRemoteException) message.getData();
			processError(message, remoteException);
		} else {
			processResponse(message);
		}
	}

	private void processError(RpcMessage message, RpcRemoteException exception) {
		ResultCallback<?> callback = getResultCallback(message);
		if (callback == null)
			return;
		returnError(callback, exception);
	}

	private void processResponse(RpcMessage message) {
		ResultCallback<Object> callback = getResultCallback(message);
		if (callback == null)
			return;
		callback.onResult(message.getData());
	}

	@SuppressWarnings("unchecked")
	private <T> ResultCallback<T> getResultCallback(RpcMessage message) {
		return (ResultCallback<T>) requests.remove(message.getCookie());
	}

	@Override
	public void ready() {
		statusListener.onOpen(this);
	}

	@Override
	public void onClosed() {
		if (scheduleExpiredResponsesTask != null)
			scheduleExpiredResponsesTask.cancel();
		if (!requests.isEmpty()) {
			closeNotify();
		}
		statusListener.onClosed();
	}

	private void closeNotify() {
		for (Integer cookie : new HashSet<>(requests.keySet())) {
			returnProtocolError(requests.remove(cookie), new RpcException("Connection closed."));
		}
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public SocketConnection getSocketConnection() {
		return protocol.getSocketConnection();
	}

	public void close() {
		closing = true;
		protocol.close();
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

	public RpcRequestStats getRequestStats() {
		return requestsStats;
	}

	private final class JmxConnectionMonitoringResultCallback<T> implements ResultCallback<T> {

		private Stopwatch stopwatch;
		private final ResultCallback<T> callback;

		public JmxConnectionMonitoringResultCallback(ResultCallback<T> callback) {
			this.stopwatch = Stopwatch.createStarted();
			this.callback = callback;
		}

		@Override
		public void onResult(T result) {
			if (isMonitoring()) {
				requestsStats.getSuccessfulRequests().recordEvent();
				requestsStats.getResponseTime().recordValue(timeElapsed());
			}
			callback.onResult(result);
		}

		@Override
		public void onException(Exception exception) {
			if (isMonitoring()) {
				if (exception instanceof RpcRemoteException) {
					requestsStats.getFailedRequests().recordEvent();
					requestsStats.getResponseTime().recordValue(timeElapsed());

					long timestamp = eventloop.currentTimeMillis();
					requestsStats.getServerExceptions().recordException(exception, null);
				}
			}
			callback.onException(exception);
		}

		private int timeElapsed() {
			return (int) (stopwatch.elapsed(TimeUnit.MILLISECONDS));
		}
	}
}
