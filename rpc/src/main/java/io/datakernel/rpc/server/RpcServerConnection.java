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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.Map;

public final class RpcServerConnection implements RpcConnection {

	public interface StatusListener {
		void onOpen(RpcServerConnection connection);

		void onClosed();
	}

	private static final Logger logger = LoggerFactory.getLogger(RpcServerConnection.class);
	private final Eventloop eventloop;
	private final RpcProtocol protocol;
	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers;
	private final StatusListener statusListener;

	// JMX
	private final ExceptionStats lastRemoteException = new ExceptionStats();
	private final ValueStats requestHandlingTime = new ValueStats();
	private EventStats successfulResponses = new EventStats();
	private EventStats errorResponses = new EventStats();
	private boolean monitoring;

	public RpcServerConnection(Eventloop eventloop, SocketChannel socketChannel,
	                           BufferSerializer<RpcMessage> messageSerializer,
	                           Map<Class<?>, RpcRequestHandler<?, ?>> handlers,
	                           RpcProtocolFactory protocolFactory, StatusListener statusListener) {
		this.eventloop = eventloop;
		this.protocol = protocolFactory.create(this, socketChannel, messageSerializer, true);
		this.handlers = handlers;
		this.statusListener = statusListener;
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
	public void onReceiveMessage(final RpcMessage message) {
		final int cookie = message.getCookie();
		final long startTime = monitoring ? System.currentTimeMillis() : 0;

		apply(message.getData(), new ResultCallback<Object>() {
			@Override
			public void onResult(Object result) {
				updateProcessTime();
				protocol.sendMessage(new RpcMessage(cookie, result));
				successfulResponses.recordEvent();
			}

			@Override
			public void onException(Exception exception) {
				updateProcessTime();
				lastRemoteException.recordException(exception, message.getData());
				sendError(cookie, exception);
			}

			private void updateProcessTime() {
				if (startTime == 0)
					return;
				int value = (int) (System.currentTimeMillis() - startTime);
				requestHandlingTime.recordValue(value);
			}
		});
	}

	private void sendError(int cookie, Exception error) {
		protocol.sendMessage(new RpcMessage(cookie, new RpcRemoteException(error)));
		logger.warn("Exception while process request ID {}", cookie, error);
		errorResponses.recordEvent();
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

	// JMX
	public void startMonitoring() {
		monitoring = true;
	}

	public void stopMonitoring() {
		monitoring = false;
	}

	@JmxAttribute
	public boolean getOverloaded() {
		return protocol.isOverloaded();
	}

	@JmxAttribute
	public EventStats getSuccessfulResponses() {
		return successfulResponses;
	}

	@JmxAttribute
	public EventStats getErrorResponses() {
		return errorResponses;
	}

	@JmxAttribute
	public ValueStats getRequestHandlingTime() {
		return requestHandlingTime;
	}

	@JmxAttribute
	public ExceptionStats getLastResponseException() {
		return lastRemoteException;
	}
}
