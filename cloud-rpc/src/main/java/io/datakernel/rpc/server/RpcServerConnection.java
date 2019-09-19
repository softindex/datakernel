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

package io.datakernel.rpc.server;

import io.datakernel.common.parse.ParseException;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.jmx.ExceptionStats;
import io.datakernel.eventloop.jmx.JmxRefreshable;
import io.datakernel.eventloop.jmx.ValueStats;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.protocol.RpcControlMessage;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcRemoteException;
import io.datakernel.rpc.protocol.RpcStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;

public final class RpcServerConnection implements RpcStream.Listener, JmxRefreshable {
	private static final Logger logger = LoggerFactory.getLogger(RpcServerConnection.class);

	private StreamDataAcceptor<RpcMessage> downstreamDataAcceptor;

	private final RpcServer rpcServer;
	private final RpcStream stream;
	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers;

	private int activeRequests = 1;

	// jmx
	private final InetAddress remoteAddress;
	private final ExceptionStats lastRequestHandlingException = ExceptionStats.create();
	private final ValueStats requestHandlingTime = ValueStats.create(RpcServer.SMOOTHING_WINDOW).withUnit("milliseconds");
	private final EventStats successfulRequests = EventStats.create(RpcServer.SMOOTHING_WINDOW);
	private final EventStats failedRequests = EventStats.create(RpcServer.SMOOTHING_WINDOW);
	private boolean monitoring = false;

	RpcServerConnection(RpcServer rpcServer, InetAddress remoteAddress,
			Map<Class<?>, RpcRequestHandler<?, ?>> handlers, RpcStream stream) {
		this.rpcServer = rpcServer;
		this.stream = stream;
		this.handlers = handlers;

		// jmx
		this.remoteAddress = remoteAddress;
	}

	@SuppressWarnings("unchecked")
	private Promise<Object> serve(Object request) {
		RpcRequestHandler<Object, Object> requestHandler = (RpcRequestHandler<Object, Object>) handlers.get(request.getClass());
		if (requestHandler == null) {
			return Promise.ofException(new ParseException(RpcServerConnection.class, "Failed to process request " + request));
		}
		return requestHandler.run(request);
	}

	@Override
	public void accept(RpcMessage message) {
		activeRequests++;

		int cookie = message.getCookie();
		long startTime = monitoring ? System.currentTimeMillis() : 0;

		Object messageData = message.getData();
		serve(messageData)
				.whenComplete((result, e) -> {
					if (startTime != 0) {
						int value = (int) (System.currentTimeMillis() - startTime);
						requestHandlingTime.recordValue(value);
						rpcServer.getRequestHandlingTime().recordValue(value);
					}
					if (e == null) {
						downstreamDataAcceptor.accept(RpcMessage.of(cookie, result));

						successfulRequests.recordEvent();
						rpcServer.getSuccessfulRequests().recordEvent();

						if (--activeRequests == 0) {
							doClose();
							stream.sendEndOfStream();
						}
					} else {
						downstreamDataAcceptor.accept(RpcMessage.of(cookie, new RpcRemoteException(e)));

						lastRequestHandlingException.recordException(e, messageData);
						rpcServer.getLastRequestHandlingException().recordException(e, messageData);
						failedRequests.recordEvent();
						rpcServer.getFailedRequests().recordEvent();

						if (--activeRequests == 0) {
							doClose();
							stream.sendEndOfStream();
						}
						logger.warn("Exception while processing request ID {}", cookie, e);
					}
				});
	}

	@Override
	public void onReceiverEndOfStream() {
		activeRequests--;
		if (activeRequests == 0) {
			doClose();
			stream.sendEndOfStream();
		}
	}

	@Override
	public void onReceiverError(@NotNull Throwable e) {
		logger.error("Receiver error: " + remoteAddress, e);
		rpcServer.getLastProtocolError().recordException(e, remoteAddress);
		doClose();
		stream.close();
	}

	@Override
	public void onSenderError(@NotNull Throwable e) {
		logger.error("Sender error: " + remoteAddress, e);
		rpcServer.getLastProtocolError().recordException(e, remoteAddress);
		doClose();
		stream.close();
	}

	@Override
	public void onSenderReady(@NotNull StreamDataAcceptor<RpcMessage> acceptor) {
		this.downstreamDataAcceptor = acceptor;
	}

	@Override
	public void onSenderSuspended() {
	}

	private void doClose() {
		rpcServer.remove(this);
	}

	public void shutdown() {
		downstreamDataAcceptor.accept(RpcMessage.of(-1, RpcControlMessage.CLOSE));
	}

	// jmx
	public void startMonitoring() {
		monitoring = true;
	}

	public void stopMonitoring() {
		monitoring = false;
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
