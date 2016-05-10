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
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.CountStats;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.RpcClientConnection.StatusListener;
import io.datakernel.rpc.client.jmx.RpcConnectStats;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcNoSenderException;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.datakernel.async.AsyncCallbacks.postCompletion;
import static io.datakernel.async.AsyncCallbacks.postException;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcClient implements EventloopService, EventloopJmxMBean {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);
	public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
	public static final int DEFAULT_RECONNECT_INTERVAL = 1 * 1000;

	private Logger logger = LoggerFactory.getLogger(RpcClient.class);

	private final Eventloop eventloop;
	private RpcStrategy strategy;
	private List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private SerializerBuilder serializerBuilder;
	private final Set<Class<?>> messageTypes = new LinkedHashSet<>();
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT;
	private int reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL;

	private BufferSerializer<RpcMessage> serializer;

	private RpcSender requestSender;

	private CompletionCallback startCallback;
	private boolean running;

	private final RpcClientConnectionPool pool = new RpcClientConnectionPool() {
		@Override
		public RpcClientConnection get(InetSocketAddress address) {
			return connections.get(address);
		}
	};

	// JMX

	private boolean monitoring;

	private final RpcRequestStats generalRequestsStats;
	private final RpcConnectStats generalConnectsStats;
	private final Map<Class<?>, RpcRequestStats> requestStatsPerClass;
	private final Map<InetSocketAddress, RpcConnectStats> connectsStatsPerAddress;

	private RpcClient(Eventloop eventloop) {
		this.eventloop = eventloop;

		// JMX
		this.generalRequestsStats = new RpcRequestStats();
		this.generalConnectsStats = new RpcConnectStats();
		this.requestStatsPerClass = new HashMap<>();
		this.connectsStatsPerAddress = new HashMap<>(); // TODO(vmykhalko): properly initialize this map with addresses, and add new addresses when needed
	}

	public static RpcClient create(final Eventloop eventloop) {
		return new RpcClient(eventloop);
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

	public RpcClient socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
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

	public void stop() {
		checkState(eventloop.inEventloopThread());
		checkState(running);
		running = false;
		if (startCallback != null) {
			postException(eventloop, startCallback, new InterruptedException("Start aborted"));
			startCallback = null;
		}
		closeConnections();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		stop();
		callback.onComplete();
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
		eventloop.connect(address, socketSettings, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				StatusListener statusListener = new StatusListener() {
					@Override
					public void onOpen(RpcClientConnection connection) {
						addConnection(address, connection);
					}

					@Override
					public void onClosed() {
						logger.info("Connection to {} closed", address);
						removeConnection(address);

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
				};
				RpcClientConnection connection = new RpcClientConnection(eventloop, socketChannel,
						getSerializer(), protocolFactory, statusListener);
				connection.getSocketConnection().register();

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
			public void onException(Exception exception) {
				//jmx
				generalConnectsStats.getFailedConnects().recordEvent();
				connectsStatsPerAddress.get(address).getFailedConnects().recordEvent();

				if (running) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, exception.toString());
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

	private void removeConnection(InetSocketAddress address) {
		connections.remove(address);
		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();
	}

	private void closeConnections() {
		for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
			connection.close();
		}
	}

	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		ResultCallback<T> requestCallback = callback;

		// jmx
		generalRequestsStats.getTotalRequests().recordEvent();
		if (isMonitoring()) {
			Class<?> requestClass = request.getClass();
			ensureRequestStatsPerClass(requestClass).getTotalRequests().recordEvent();
			requestCallback = new JmxMonitoringResultCallback<>(requestClass, callback);
		}

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
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}
	}

	// JMX
	@JmxOperation
	public void startMonitoring() {
		RpcClient.this.monitoring = true;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.startMonitoring();
			}
		}
	}

	@JmxOperation
	public void stopMonitoring() {
		RpcClient.this.monitoring = false;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.stopMonitoring();
			}
		}
	}

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

	@JmxAttribute
	public Map<Class<?>, RpcRequestStats> getRequestsStatsPerClass() {
		return requestStatsPerClass;
	}

	@JmxAttribute
	public Map<InetSocketAddress, RpcConnectStats> getConnectsStatsPerAddress() {
		return connectsStatsPerAddress;
	}

	@JmxAttribute
	public Map<InetSocketAddress, RpcRequestStats> getRequestStatsPerAddress() {
		Map<InetSocketAddress, RpcRequestStats> requestStatsPerAddress = new HashMap<>();
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				RpcRequestStats stats = connection.getRequestStats();
				requestStatsPerAddress.put(address, stats);
			}
		}
		return requestStatsPerAddress;
	}

	@JmxAttribute(name = "activeConnections")
	public CountStats getActiveConnectionsCount() {
		CountStats countStats = new CountStats();
		countStats.setCount(connections.size());
		return countStats;
	}

	private RpcRequestStats ensureRequestStatsPerClass(Class<?> requestClass) {
		if (!requestStatsPerClass.containsKey(requestClass)) {
			requestStatsPerClass.put(requestClass, new RpcRequestStats());
		}
		return requestStatsPerClass.get(requestClass);
	}

	private final class JmxMonitoringResultCallback<T> implements ResultCallback<T> {

		private Stopwatch stopwatch;
		private final Class<?> requestClass;
		private final ResultCallback<T> callback;

		public JmxMonitoringResultCallback(Class<?> requestClass, ResultCallback<T> callback) {
			this.stopwatch = Stopwatch.createStarted();
			this.requestClass = requestClass;
			this.callback = callback;
		}

		@Override
		public void onResult(T result) {
			if (isMonitoring()) {
				generalRequestsStats.getSuccessfulRequests().recordEvent();
				ensureRequestStatsPerClass(requestClass).getSuccessfulRequests().recordEvent();
				updateResponseTime(requestClass, timeElapsed());
			}
			callback.onResult(result);
		}

		@Override
		public void onException(Exception exception) {
			if (isMonitoring()) {
				if (exception instanceof RpcTimeoutException) {
					generalRequestsStats.getExpiredRequests().recordEvent();
					ensureRequestStatsPerClass(requestClass).getExpiredRequests().recordEvent();
				} else if (exception instanceof RpcOverloadException) {
					generalRequestsStats.getRejectedRequests().recordEvent();
					ensureRequestStatsPerClass(requestClass).getRejectedRequests().recordEvent();
				} else if (exception instanceof RpcRemoteException) {
					generalRequestsStats.getFailedRequests().recordEvent();
					ensureRequestStatsPerClass(requestClass).getFailedRequests().recordEvent();
					updateResponseTime(requestClass, timeElapsed());

					long timestamp = eventloop.currentTimeMillis();
					generalRequestsStats.getServerExceptions().recordException(exception, null);
					ensureRequestStatsPerClass(requestClass)
							.getServerExceptions().recordException(exception, null);
				}
			}
			callback.onException(exception);
		}

		private void updateResponseTime(Class<?> requestClass, int responseTime) {
			generalRequestsStats.getResponseTime().recordValue(responseTime);
			ensureRequestStatsPerClass(requestClass).getResponseTime().recordValue(responseTime);
		}

		private int timeElapsed() {
			return (int) (stopwatch.elapsed(TimeUnit.MILLISECONDS));
		}
	}
}
