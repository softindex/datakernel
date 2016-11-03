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

import io.datakernel.async.*;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.jmx.RpcConnectStats;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcNoSenderException;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.*;
import static java.lang.ClassLoader.getSystemClassLoader;

public final class RpcClient implements IRpcClient, EventloopService, EventloopJmxMBean {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create().withTcpNoDelay(true);
	public static final long DEFAULT_CONNECT_TIMEOUT = 10 * 1000L;
	public static final long DEFAULT_RECONNECT_INTERVAL = 1 * 1000L;

	private final Logger logger;

	private final Eventloop eventloop;
	private final SocketSettings socketSettings;

	// SSL
	private final SSLContext sslContext;
	private final ExecutorService sslExecutor;

	private final RpcStrategy strategy;
	private final List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private final RpcProtocolFactory protocolFactory;
	private final List<Class<?>> messageTypes;
	private final long connectTimeoutMillis;
	private final long reconnectIntervalMillis;
	private final boolean forceStart;

	private final SerializerBuilder serializerBuilder;
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
	private final RpcRequestStats generalRequestsStats = RpcRequestStats.create();
	private final RpcConnectStats generalConnectsStats = RpcConnectStats.create();
	private final Map<Class<?>, RpcRequestStats> requestStatsPerClass = new HashMap<>();
	private final Map<InetSocketAddress, RpcConnectStats> connectsStatsPerAddress = new HashMap<>();
	private final ExceptionStats lastProtocolError = ExceptionStats.create();

	// region builders
	private RpcClient(Eventloop eventloop, SocketSettings socketSettings, List<Class<?>> messageTypes,
	                  SerializerBuilder serializerBuilder, RpcStrategy strategy, RpcProtocolFactory protocol,
	                  Logger logger, long connectTimeoutMillis, long reconnectIntervalMillis,
	                  boolean forceStart, SSLContext sslContext, ExecutorService executor) {
		this.eventloop = eventloop;
		this.socketSettings = socketSettings;
		this.messageTypes = messageTypes;
		this.serializerBuilder = serializerBuilder;
		this.protocolFactory = protocol;
		this.logger = logger;
		this.connectTimeoutMillis = connectTimeoutMillis;
		this.reconnectIntervalMillis = reconnectIntervalMillis;
		this.forceStart = forceStart;
		this.sslContext = sslContext;
		this.sslExecutor = executor;
		this.strategy = strategy;
		this.addresses = new ArrayList<>(strategy.getAddresses());

		// jmx
		for (InetSocketAddress address : this.addresses) {
			if (!connectsStatsPerAddress.containsKey(address)) {
				connectsStatsPerAddress.put(address, RpcConnectStats.create());
			}
		}
	}

	@SuppressWarnings("ConstantConditions")
	public static RpcClient create(final Eventloop eventloop) {
		checkNotNull(eventloop);

		List<Class<?>> defaultMessageTypes = null;
		SerializerBuilder serializerBuilder = SerializerBuilder.create(getSystemClassLoader());
		RpcStrategy defaultStrategy = new NoServersStrategy();
		RpcStreamProtocolFactory defaultProtocol = streamProtocol();
		Logger defaultLogger = LoggerFactory.getLogger(RpcClient.class);
		SSLContext nullSslContext = null;
		ExecutorService nullSslExecutor = null;

		return new RpcClient(
				eventloop, DEFAULT_SOCKET_SETTINGS, defaultMessageTypes, serializerBuilder,
				defaultStrategy, defaultProtocol, defaultLogger,
				DEFAULT_CONNECT_TIMEOUT, DEFAULT_RECONNECT_INTERVAL, false,
				nullSslContext, nullSslExecutor);
	}

	public RpcClient withSocketSettings(SocketSettings socketSettings) {
		checkNotNull(socketSettings);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	public RpcClient withMessageTypes(Class<?>... messageTypes) {
		checkNotNull(messageTypes);
		return withMessageTypes(Arrays.asList(messageTypes));
	}

	public RpcClient withMessageTypes(List<Class<?>> messageTypes) {
		checkNotNull(messageTypes);
		checkArgument(new HashSet<>(messageTypes).size() == messageTypes.size(), "Message types must be unique");
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	public RpcClient withSerializerBuilder(SerializerBuilder serializerBuilder) {
		checkNotNull(serializerBuilder);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	public RpcClient withStrategy(RpcStrategy requestSendingStrategy) {
		checkNotNull(requestSendingStrategy);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				requestSendingStrategy, protocolFactory, logger,
				connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	public RpcClient withProtocol(RpcProtocolFactory protocolFactory) {
		checkNotNull(protocolFactory);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	public RpcClient withStreamProtocol(int defaultPacketSize, int maxPacketSize, boolean compression) {
		return withProtocol(streamProtocol(defaultPacketSize, maxPacketSize, compression));
	}

	public RpcClient withStreamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		return withProtocol(streamProtocol(defaultPacketSize, maxPacketSize, compression));
	}

	public RpcClient withConnectTimeout(long connectTimeoutMillis) {
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	public RpcClient withReconnectInterval(long reconnectIntervalMillis) {
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	public RpcClient withSslEnabled(SSLContext sslContext, ExecutorService executor) {
		checkNotNull(sslContext);
		checkNotNull(executor);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, executor);
	}

	public RpcClient withLogger(Logger logger) {
		checkNotNull(logger);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, forceStart,
				sslContext, sslExecutor);
	}

	/**
	 * Start RpcClient in case of absence of connections
	 *
	 * @return
	 */
	public RpcClient withForceStart() {
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis, true,
				sslContext, sslExecutor);
	}
	// endregion

	public SocketSettings getSocketSettings() {
		return socketSettings;
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

		if (forceStart) {
			startCallback.postComplete(eventloop);
			RpcSender sender = strategy.createSender(pool);
			requestSender = sender != null ? sender : new Sender();
			startCallback = null;
		} else {
			if (connectTimeoutMillis != 0) {
				eventloop.scheduleBackground(eventloop.currentTimeMillis() + connectTimeoutMillis, new Runnable() {
					@Override
					public void run() {
						if (running && startCallback != null) {
							String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
									connectTimeoutMillis / 1000.0);
							startCallback.postException(eventloop, new InterruptedException(errorMsg));
							running = false;
							startCallback = null;
						}
					}
				});
			}
		}

		for (InetSocketAddress address : addresses) {
			connect(address);
		}
	}

	public Future<Void> startFuture() {
		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				start(future);
			}
		});
		return future;
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		checkState(eventloop.inEventloopThread());
		checkState(running);

		running = false;
		if (startCallback != null) {
			startCallback.postException(eventloop, new InterruptedException("Start aborted"));
			startCallback = null;
		}

		if (connections.size() == 0) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					callback.setComplete();
				}
			});
		} else {
			stopCallback = callback;
			for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
				connection.close();
			}
		}
	}

	public Future<Void> stopFuture() {
		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				stop(future);
			}
		});
		return future;
	}

	private BufferSerializer<RpcMessage> getSerializer() {
		checkState(messageTypes != null, "Message types must be specified");
		if (serializer == null) {
			serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);
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
				RpcClientConnection connection = RpcClientConnection.create(eventloop, RpcClient.this,
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
					startCallback.postComplete(eventloop);
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

	void removeConnection(final InetSocketAddress address) {
		logger.info("Connection to {} closed", address);

		connections.remove(address);

		if (stopCallback != null && connections.size() == 0) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					stopCallback.setComplete();
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

	@Override
	public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
		requestSender.sendRequest(request, timeout, callback);
	}

	public IRpcClient adaptToAnotherEventloop(final Eventloop anotherEventloop) {
		if (anotherEventloop == this.eventloop) {
			return this;
		}

		return new IRpcClient() {
			@Override
			public <I, O> void sendRequest(final I request, final int timeout, final ResultCallback<O> callback) {
				RpcClient.this.eventloop.execute(new Runnable() {
					@Override
					public void run() {
						RpcClient.this.sendRequest(
								request, timeout, ConcurrentResultCallback.create(callback, eventloop)
						);
					}
				});
			}
		};
	}

	// visible for testing
	public RpcSender getRequestSender() {
		return requestSender;
	}

	private final class Sender implements RpcSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private final RpcNoSenderException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderException("No senders available");

		@Override
		public <I, O> void sendRequest(I request, int timeout, final ResultCallback<O> callback) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					callback.setException(NO_SENDER_AVAILABLE_EXCEPTION);
				}
			});
		}
	}

	private static final class NoServersStrategy implements RpcStrategy {

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return Collections.emptySet();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return null;
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
		CountStats countStats = CountStats.create();
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
			requestStatsPerClass.put(requestClass, RpcRequestStats.create());
		}
		return requestStatsPerClass.get(requestClass);
	}
}
