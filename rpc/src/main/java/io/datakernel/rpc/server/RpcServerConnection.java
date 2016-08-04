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

import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;

public final class RpcServerConnection implements RpcConnection, JmxRefreshable {
	private static final Logger logger = LoggerFactory.getLogger(RpcServerConnection.class);

	private final Eventloop eventloop;
	private final RpcServer rpcServer;
	private final RpcProtocol protocol;
	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers;

	// jmx
	private final InetSocketAddress remoteAddress;
	private final ExceptionStats lastRequestHandlingException = new ExceptionStats();
	private final ValueStats requestHandlingTime = new ValueStats();
	private EventStats successfulRequests = new EventStats();
	private EventStats failedRequests = new EventStats();
	private boolean monitoring = false;

	public RpcServerConnection(Eventloop eventloop, RpcServer rpcServer, AsyncTcpSocket asyncTcpSocket,
	                           BufferSerializer<RpcMessage> messageSerializer,
	                           Map<Class<?>, RpcRequestHandler<?, ?>> handlers,
	                           RpcProtocolFactory protocolFactory) {
		this.eventloop = eventloop;
		this.rpcServer = rpcServer;
		this.protocol = protocolFactory.create(eventloop, asyncTcpSocket, this, messageSerializer);
		this.handlers = handlers;

		// jmx
		this.remoteAddress = asyncTcpSocket.getRemoteSocketAddress();
	}

	@SuppressWarnings("unchecked")
	private void apply(Object request, ResultCallback<Object> callback) {
		RpcRequestHandler requestHandler = handlers.get(request.getClass());
		if (requestHandler == null) {
			callback.onException(new ParseException("Failed to process request " + request));
			return;
		}
		requestHandler.run(request, callback);
	}

	@Override
	public void onData(final RpcMessage message) {
		final int cookie = message.getCookie();
		final long startTime = monitoring ? System.currentTimeMillis() : 0;

		final Object messageData = message.getData();
		apply(messageData, new ResultCallback<Object>() {
			@Override
			public void onResult(Object result) {
				// jmx
				updateProcessTime();
				successfulRequests.recordEvent();
				rpcServer.getSuccessfulRequests().recordEvent();

				protocol.sendMessage(new RpcMessage(cookie, result));
			}

			@Override
			public void onException(Exception exception) {
				// jmx
				updateProcessTime();
				lastRequestHandlingException.recordException(exception, messageData);
				rpcServer.getLastRequestHandlingException().recordException(exception, messageData);
				failedRequests.recordEvent();
				rpcServer.getFailedRequests().recordEvent();

				sendError(cookie, exception);
			}

			private void updateProcessTime() {
				if (startTime == 0)
					return;
				int value = (int) (System.currentTimeMillis() - startTime);
				requestHandlingTime.recordValue(value);
				rpcServer.getRequestHandlingTime().recordValue(value);
			}
		});
	}

	private void sendError(int cookie, Exception error) {
		protocol.sendMessage(new RpcMessage(cookie, new RpcRemoteException(error)));
		logger.warn("Exception while process request ID {}", cookie, error);
	}

	@Override
	public void onClosed() {
		rpcServer.remove(this);
	}

	@Override
	public AsyncTcpSocket.EventHandler getSocketConnection() {
		return protocol.getSocketConnection();
	}

	public void close() {
		protocol.close();
	}

	// jmx
	public void startMonitoring() {
		monitoring = true;
	}

	public void stopMonitoring() {
		monitoring = false;
	}

	@JmxAttribute
	public boolean isOverloaded() {
		return protocol.isOverloaded();
	}

	@JmxAttribute
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute
	public ValueStats getRequestHandlingTime() {
		return requestHandlingTime;
	}

	@JmxAttribute
	public ExceptionStats getLastRequestHandlingException() {
		return lastRequestHandlingException;
	}

	@JmxAttribute
	public String getRemoteAddress() {
		return remoteAddress.toString();
	}

	@Override
	public void refresh(long timestamp) {
		successfulRequests.refresh(timestamp);
		failedRequests.refresh(timestamp);
		requestHandlingTime.refresh(timestamp);
	}
}
