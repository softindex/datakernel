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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.*;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.jmx.RpcConnectStats;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcNoSenderException;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.postCompletion;
import static io.datakernel.async.AsyncCallbacks.postException;
import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcClient implements EventloopService, EventloopJmxMBean {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);
	public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
	public static final int DEFAULT_RECONNECT_INTERVAL = 1 * 1000;

	private Logger logger = LoggerFactory.getLogger(RpcClient.class);

	private final Eventloop eventloop;
	private final SocketSettings socketSettings;

	// SSL
	private SSLContext sslContext;
	private ExecutorService sslExecutor;

	private RpcStrategy strategy;
	private List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private SerializerBuilder serializerBuilder;
	private final Set<Class<?>> messageTypes = new LinkedHashSet<>();
	private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT;
	private int reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL;

	private BufferSerializer<RpcMessage> serializer;

	private RpcSender requestSender;

	private CompletionCallback startCallback;
	private CompletionCallback stopCallback;
	private boolean running;

	private final RpcClientConnectionPool pool = new RpcClientConnectionPool() {
		@Override
		public RpcClientConnection get(InetSocketAddress address) {
			return connections.get(address);
		}
	};

	// jmx
	private boolean monitoring = false;
	private final RpcRequestStats generalRequestsStats = new RpcRequestStats();
	private final RpcConnectStats generalConnectsStats = new RpcConnectStats();
	private final Map<Class<?>, RpcRequestStats> requestStatsPerClass = new HashMap<>();
	private final Map<InetSocketAddress, RpcConnectStats> connectsStatsPerAddress = new HashMap<>();
	private final ExceptionStats lastProtocolError = new ExceptionStats();

	private RpcClient(Eventloop eventloop, SocketSettings socketSettings) {
		this.eventloop = eventloop;
		this.socketSettings = socketSettings;
	}

	public static RpcClient create(final Eventloop eventloop) {
		return create(eventloop, DEFAULT_SOCKET_SETTINGS);
	}

	public static RpcClient create(final Eventloop eventloop, SocketSettings socketSettings) {
		return new RpcClient(eventloop, socketSettings);
	}

	public RpcClient messageTypes(Class<?>... messageTypes) {
		return messageTypes(Arrays.asList(messageTypes));
	}

	public RpcClient messageTypes(List<Class<?>> messageTypes) {
		this.messageTypes.addAll(messageTypes);
		return this;
	}

	public RpcClient serializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return this;
	}

	public RpcClient strategy(RpcStrategy requestSendingStrategy) {
		checkState(this.strategy == null, "Strategy is already set");

		this.strategy = requestSendingStrategy;
		this.addresses = new ArrayList<>(requestSendingStrategy.getAddresses());

		// jmx
		for (InetSocketAddress address : this.addresses) {
			if (!connectsStatsPerAddress.containsKey(address)) {
				connectsStatsPerAddress.put(address, new RpcConnectStats());
			}
		}

		return this;
	}

	public RpcClient protocol(RpcProtocolFactory protocolFactory) {
		this.protocolFactory = protocolFactory;
		return this;
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	public RpcClient logger(Logger logger) {
		this.logger = checkNotNull(logger);
		return this;
	}

	public RpcClient connectTimeoutMillis(int connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
		return this;
	}

	public RpcClient reconnectIntervalMillis(int reconnectIntervalMillis) {
		this.reconnectIntervalMillis = reconnectIntervalMillis;
		return this;
	}

	public RpcClient enableSsl(SSLContext sslContext, ExecutorService executor) {
		this.sslContext = checkNotNull(sslContext);
		this.sslExecutor = checkNotNull(executor);
		return this;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		checkNotNull(callback);
		checkState(!running);
		running = true;
		startCallback = callback;
		if (connectTimeoutMillis != 0) {
			eventloop.scheduleBackground(eventloop.currentTimeMillis() + connectTimeoutMillis, new Runnable() {
				@Override
				public void run() {
					if (running && startCallback != null) {
						String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
								connectTimeoutMillis / 1000.0);
						postException(eventloop, startCallback, new InterruptedException(errorMsg));
						running = false;
						startCallback = null;
					}
				}
			});
		}

		for (InetSocketAddress address : addresses) {
			connect(address);
		}
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		checkState(eventloop.inEventloopThread());
		checkState(running);

		running = false;
		if (startCallback != null) {
			postException(eventloop, startCallback, new InterruptedException("Start aborted"));
			startCallback = null;
		}

		if (connections.size() == 0) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					callback.complete();
				}
			});
		} else {
			stopCallback = callback;
			for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
				connection.close();
			}
		}
	}

	private BufferSerializer<RpcMessage> getSerializer() {
		if (serializer == null) {
			SerializerBuilder serializerBuilder = this.serializerBuilder != null ?
					this.serializerBuilder :
					SerializerBuilder.newDefaultInstance(ClassLoader.getSystemClassLoader());
			serializerBuilder.setExtraSubclasses("extraRpcMessageData", messageTypes);
			serializer = serializerBuilder.create(RpcMessage.class);
		}
		return serializer;
	}

	private void connect(final InetSocketAddress address) {
		if (!running) {
			return;
		}

		logger.info("Connecting {}", address);

		eventloop.connect(address, 0, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
				AsyncTcpSocket asyncTcpSocket = sslContext != null ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
				RpcClientConnection connection = new RpcClientConnection(eventloop, RpcClient.this,
						asyncTcpSocket, address,
						getSerializer(), protocolFactory);
				asyncTcpSocket.setEventHandler(connection.getSocketConnection());
				asyncTcpSocketImpl.register();

				addConnection(address, connection);

				// jmx
				generalConnectsStats.getSuccessfulConnects().recordEvent();
				connectsStatsPerAddress.get(address).getSuccessfulConnects().recordEvent();

				logger.info("Connection to {} established", address);
				if (startCallback != null) {
					postCompletion(eventloop, startCallback);
					startCallback = null;
				}
			}

			@Override
			public void onException(Exception e) {
				//jmx
				generalConnectsStats.getFailedConnects().recordEvent();
				connectsStatsPerAddress.get(address).getFailedConnects().recordEvent();

				if (running) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, e.toString());
					}
					eventloop.scheduleBackground(eventloop.currentTimeMillis() + reconnectIntervalMillis, new Runnable() {
						@Override
						public void run() {
							if (running) {
								connect(address);
							}
						}
					});
				}
			}
		});
	}

	private void addConnection(InetSocketAddress address, RpcClientConnection connection) {
		connections.put(address, connection);

		// jmx
		if (isMonitoring()) {
			connection.startMonitoring();
		}
		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();
	}

	public void removeConnection(final InetSocketAddress address) {
		logger.info("Connection to {} closed", address);

		connections.remove(address);

		if (stopCallback != null && connections.size() == 0) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					stopCallback.complete();
					stopCallback = null;
				}
			});
		}

		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();

		// jmx
		generalConnectsStats.getClosedConnects().recordEvent();
		connectsStatsPerAddress.get(address).getClosedConnects().recordEvent();

		eventloop.scheduleBackground(eventloop.currentTimeMillis() + reconnectIntervalMillis, new Runnable() {
			@Override
			public void run() {
				if (running) {
					connect(address);
				}
			}
		});
	}

	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		ResultCallback<T> requestCallback = callback;
		requestSender.sendRequest(request, timeout, requestCallback);

	}

	public <T> ResultCallbackFuture<T> sendRequestFuture(final Object request, final int timeout) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				sendRequest(request, timeout, future);
			}
		});
		return future;
	}

	// visible for testing
	public RpcSender getRequestSender() {
		return requestSender;
	}

	static final class Sender implements RpcSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderException("No senders available");

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			callback.fireException(NO_SENDER_AVAILABLE_EXCEPTION);
		}
	}

	// jmx
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled) ]")
	public void startMonitoring() {
		monitoring = true;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.startMonitoring();
			}
		}
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled) ]")
	public void stopMonitoring() {
		monitoring = false;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.stopMonitoring();
			}
		}
	}

	@JmxAttribute(description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled)")
	private boolean isMonitoring() {
		return monitoring;
	}

	@JmxOperation
	public void resetStats() {
		generalRequestsStats.resetStats();
		for (InetSocketAddress address : connectsStatsPerAddress.keySet()) {
			connectsStatsPerAddress.get(address).reset();
		}
		for (Class<?> requestClass : requestStatsPerClass.keySet()) {
			requestStatsPerClass.get(requestClass).resetStats();
		}

		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.resetStats();
			}
		}
	}

	@JmxAttribute(name = "requests")
	public RpcRequestStats getGeneralRequestsStats() {
		return generalRequestsStats;
	}

	@JmxAttribute(name = "connects")
	public RpcConnectStats getGeneralConnectsStats() {
		return generalConnectsStats;
	}

	@JmxAttribute(description = "request stats distributed by request class")
	public Map<Class<?>, RpcRequestStats> getRequestsStatsPerClass() {
		return requestStatsPerClass;
	}

	@JmxAttribute
	public Map<InetSocketAddress, RpcConnectStats> getConnectsStatsPerAddress() {
		return connectsStatsPerAddress;
	}

	@JmxAttribute(description = "request stats for current connections (when connection is closed stats are removed)")
	public Map<InetSocketAddress, RpcClientConnection> getRequestStatsPerConnection() {
		return connections;
	}

	@JmxAttribute(name = "activeConnections")
	public CountStats getActiveConnectionsCount() {
		CountStats countStats = new CountStats();
		countStats.setCount(connections.size());
		return countStats;
	}

	@JmxAttribute(description = "exception that occurred because of protocol error " +
			"(serialization, deserialization, compression, decompression, etc)")
	public ExceptionStats getLastProtocolError() {
		return lastProtocolError;
	}

	RpcRequestStats ensureRequestStatsPerClass(Class<?> requestClass) {
		if (!requestStatsPerClass.containsKey(requestClass)) {
			requestStatsPerClass.put(requestClass, new RpcRequestStats());
		}
		return requestStatsPerClass.get(requestClass);
	}
}
