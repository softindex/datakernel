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

package io.datakernel.rpc.server;

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.stats.ExceptionStats;
import io.datakernel.jmx.stats.ValueStats;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.nio.channels.SocketChannel;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcServerConnection implements RpcConnection {

	public interface StatusListener {
		void onOpen(RpcServerConnection connection);

		void onClosed();
	}

	private static final Logger logger = LoggerFactory.getLogger(RpcServerConnection.class);
	private final Eventloop eventloop;
	private final RpcProtocol protocol;
	private final Map<Class<?>, RpcRequestHandler<Object>> handlers;
	private final StatusListener statusListener;

	// JMX
	// TODO(vmykhalko): remove this marker ?
	private static final Marker LAST_REMOTE_EXCEPTION_MARKER = MarkerFactory.getMarker("RemoteException");
	private final ExceptionStats lastRemoteException = new ExceptionStats();
	private final ExceptionStats lastInternalException = new ExceptionStats();
	private final ValueStats timeExecution;
	private int successfulResponses = 0;
	private int errorResponses = 0;
	private boolean monitoring;

	public RpcServerConnection(Eventloop eventloop, SocketChannel socketChannel,
	                           BufferSerializer<RpcMessage> messageSerializer,
	                           Map<Class<?>, RpcRequestHandler<Object>> handlers,
	                           RpcProtocolFactory protocolFactory, StatusListener statusListener) {
		this.eventloop = eventloop;
		this.protocol = protocolFactory.create(this, socketChannel, messageSerializer, true);
		this.handlers = handlers;
		this.statusListener = statusListener;

		// JMX
		this.timeExecution = new ValueStats();
	}

	public void apply(Object request, ResultCallback<Object> callback) {
		RpcRequestHandler<Object> requestHandler;
		try {
			checkNotNull(request);
			checkNotNull(callback);
			requestHandler = handlers.get(request.getClass());
			checkNotNull(requestHandler, "Unknown request class: %", request.getClass());
		} catch (Exception e) {
			if (logger != null) {
				logger.error("Failed to process request " + request, e);
			}
			callback.onException(e);
			return;
		}
		requestHandler.run(request, callback);
	}

	@Override
	public void onReceiveMessage(final RpcMessage message) {
		final int cookie = message.getCookie();
		final long startTime = monitoring ? System.currentTimeMillis() : 0;

		apply(message.getData(), new ResultCallback<Object>() {
			@Override
			public void onResult(Object result) {
				updateProcessTime();
				try {
					protocol.sendMessage(new RpcMessage(cookie, result));
				} catch (Exception e) {
					onException(e);
					return;
				}
				successfulResponses++;
			}

			@Override
			public void onException(Exception exception) {
				updateProcessTime();
				lastRemoteException.recordException(exception, message.getData(), eventloop.currentTimeMillis());
				sendError(cookie, exception);
			}

			private void updateProcessTime() {
				if (startTime == 0)
					return;
				int value = (int) (System.currentTimeMillis() - startTime);
				timeExecution.recordValue(value);
			}
		});
	}

	private void sendError(int cookie, Exception error) {
		try {
			protocol.sendMessage(new RpcMessage(cookie, new RpcRemoteException(error)));
			logger.error(LAST_REMOTE_EXCEPTION_MARKER, "Exception while process request ID {}", cookie, error);
			errorResponses++;
		} catch (Exception exception) {
			lastInternalException.recordException(exception, error, eventloop.currentTimeMillis());
			throw new RuntimeException(exception);
		}
	}

	@Override
	public void ready() {
		statusListener.onOpen(this);
	}

	@Override
	public void onClosed() {
		statusListener.onClosed();
	}

	public void close() {
		protocol.close();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public SocketConnection getSocketConnection() {
		return protocol.getSocketConnection();
	}

//	// JMX
//	// TODO(vmykhalko): upgrade jmx here
//	public void startMonitoring() {
//		monitoring = true;
//		protocol.startMonitoring();
//	}
//
//	public void stopMonitoring() {
//		monitoring = false;
//		protocol.stopMonitoring();
//	}
//
//	public boolean isMonitoring() {
//		return monitoring;
//	}
//
//	public void resetStats() {
//		lastRemoteException.resetStats();
//		lastInternalException.resetStats();
//		successfulResponses = 0;
//		errorResponses = 0;
//		timeExecution.resetStats();
//		protocol.reset();
//	}
//
//	public void refreshStats(long timestamp, double smoothingWindow) {
//		timeExecution.refreshStats(timestamp, smoothingWindow);
//	}
//
////	public CompositeData getConnectionDetails() throws OpenDataException {
////		return protocol.getConnectionDetails();
////	}
//
//	public int getSuccessfulResponses() {
//		return successfulResponses;
//	}
//
//	public int getErrorResponses() {
//		return errorResponses;
//	}
//
//	public String getTimeExecutionMillis() {
//		return timeExecution.toString();
//	}
//
//	public CompositeData getLastResponseException() throws OpenDataException {
//		return lastRemoteException.compositeData();
//	}
//
//	public CompositeData getLastInternalException() throws OpenDataException {
//		return lastInternalException.compositeData();
//	}
}
