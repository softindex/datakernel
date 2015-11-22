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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.DynamicStatsCounter;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.jmx.StatsCounter;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.slf4j.LoggerFactory.getLogger;

public final class RpcClientConnectionImpl implements RpcClientConnection, RpcClientConnectionMBean {
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

	private static final Logger logger = getLogger(RpcClientConnection.class);
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final RpcException OVERLOAD_EXCEPTION = new RpcException("Write connection is overloaded");
	private final NioEventloop eventloop;
	private final RpcProtocol protocol;
	private final StatusListener statusListener;
	private final Map<Integer, ResultCallback<?>> requests = new HashMap<>();
	private final PriorityQueue<TimeoutCookie> timeoutCookies = new PriorityQueue<>();
	private final Runnable expiredResponsesTask = createExpiredResponsesTask();

	private AsyncCancellable scheduleExpiredResponsesTask;
	private int cookieCounter = 0;
	private boolean closing;

	// JMX
	private final DynamicStatsCounter pendingRequests = new DynamicStatsCounter(1 << 10);
	private final StatsCounter timeProcessResult = new StatsCounter();
	private final StatsCounter timeProcessException = new StatsCounter();
	private final StatsCounter timeSendPacket = new StatsCounter();
	private final LastExceptionCounter lastTimeoutException = new LastExceptionCounter("TimeoutException");
	private final LastExceptionCounter lastRemoteException = new LastExceptionCounter("RemoteException");
	private final LastExceptionCounter lastInternalException = new LastExceptionCounter("InternalException");
	private int successfulRequests, failedRequests, rejectedRequests, expiredRequests;
	private boolean monitoring;

	public RpcClientConnectionImpl(NioEventloop eventloop, SocketChannel socketChannel,
	                               BufferSerializer<RpcMessage> messageSerializer,
	                               RpcProtocolFactory protocolFactory, StatusListener statusListener) {
		this.eventloop = eventloop;
		this.statusListener = statusListener;
		this.protocol = protocolFactory.create(this, socketChannel, messageSerializer, false);
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
		assert eventloop.inEventloopThread();

		if (!(request instanceof RpcMandatoryData) && protocol.isOverloaded()) {
			rejectedRequests++;
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
		TimeoutCookie timeoutCookie = new TimeoutCookie(cookieCounter, timeout);
		addTimeoutCookie(timeoutCookie);
		requests.put(cookieCounter, callback);
		pendingRequests.add(requests.size());
		Stopwatch stopwatch = monitoring ? Stopwatch.createStarted() : null;
		try {
			protocol.sendMessage(new RpcMessage(cookieCounter, request));
		} catch (Exception e) {
			if (logger.isErrorEnabled())
				logger.error("Send RpcMessage {} failed", request, e);
			removeTimeoutCookie(timeoutCookie);
			returnProtocolError(requests.remove(cookieCounter), e);
		} finally {
			if (stopwatch != null)
				timeSendPacket.add((int) stopwatch.elapsed(MICROSECONDS));
		}
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
		expiredRequests++;
		returnTimeout(callback, new RpcTimeoutException("Timeout (" + timeoutCookie.getElapsedTime() + "/" + timeoutCookie.getTimeoutMillis()
				+ " ms) for server response for request ID " + timeoutCookie.getCookie()));
	}

	private void removeTimeoutCookie(TimeoutCookie timeoutCookie) {
		timeoutCookies.remove(timeoutCookie);
	}

	private void returnTimeout(ResultCallback<?> callback, Exception exception) {
		lastTimeoutException.update(exception, null, eventloop.currentTimeMillis());
		returnError(callback, exception);
	}

	private void returnProtocolError(ResultCallback<?> callback, Exception exception) {
		lastInternalException.update(exception, null, eventloop.currentTimeMillis());
		returnError(callback, exception);
	}

	private void returnError(ResultCallback<?> callback, Exception exception) {
		failedRequests++;
		if (callback != null) {
			Stopwatch stopwatch = (monitoring) ? Stopwatch.createStarted() : null;
			callback.onException(exception);
			if (stopwatch != null)
				timeProcessException.add((int) stopwatch.elapsed(MICROSECONDS));
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
		lastRemoteException.update(exception, message, eventloop.currentTimeMillis());
		ResultCallback<?> callback = getResultCallback(message);
		if (callback == null)
			return;
		returnError(callback, exception);
	}

	private void processResponse(RpcMessage message) {
		ResultCallback<Object> callback = getResultCallback(message);
		if (callback == null)
			return;
		successfulRequests++;
		Stopwatch stopwatch = monitoring ? Stopwatch.createStarted() : null;
		callback.onResult(message.getData());
		if (stopwatch != null)
			timeProcessResult.add((int) stopwatch.elapsed(MICROSECONDS));
	}

	@SuppressWarnings("unchecked")
	private <T> ResultCallback<T> getResultCallback(RpcMessage message) {
		return (ResultCallback<T>) requests.remove(message.getCookie());
	}

	@Override
	public void close() {
		closing = true;
		protocol.close();
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
	public NioEventloop getEventloop() {
		return eventloop;
	}

	@Override
	public SocketConnection getSocketConnection() {
		return protocol.getSocketConnection();
	}

	// JMX
	@Override
	public void startMonitoring() {
		monitoring = true;
		protocol.startMonitoring();
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		protocol.stopMonitoring();
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void reset() {
		lastTimeoutException.reset();
		lastRemoteException.reset();
		lastInternalException.reset();
		pendingRequests.reset();
		successfulRequests = 0;
		failedRequests = 0;
		rejectedRequests = 0;
		expiredRequests = 0;
		timeProcessException.reset();
		timeProcessResult.reset();
		timeSendPacket.reset();
		protocol.reset();
	}

	@Override
	public CompositeData getConnectionDetails() throws OpenDataException {
		return protocol.getConnectionDetails();
	}

	@Override
	public String getPendingRequestsStats() {
		return pendingRequests.toString();
	}

	@Override
	public int getPendingRequests() {
		return requests.size();
	}

	@Override
	public int getSuccessfulRequests() {
		return successfulRequests;
	}

	@Override
	public int getFailedRequests() {
		return failedRequests;
	}

	@Override
	public int getRejectedRequests() {
		return rejectedRequests;
	}

	@Override
	public int getExpiredRequests() {
		return expiredRequests;
	}

	@Override
	public String getProcessResultTimeStats() {
		return timeProcessResult.toString();
	}

	@Override
	public String getProcessExceptionTimeStats() {
		return timeProcessException.toString();
	}

	@Override
	public String getSendPacketTimeStats() {
		return timeSendPacket.toString();
	}

	@Override
	public CompositeData getLastTimeoutException() {
		return lastTimeoutException.compositeData();
	}

	@Override
	public CompositeData getLastProtocolException() {
		return lastInternalException.compositeData();
	}

	@Override
	public CompositeData getLastRemoteException() {
		return lastRemoteException.compositeData();
	}
}
