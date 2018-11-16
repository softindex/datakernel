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

package io.datakernel.rpc.client;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Callback;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.csp.process.ChannelBinarySerializer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.AsyncTcpSocketImpl.JmxInspector;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.jmx.RpcConnectStats;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategies;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcStream;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.util.Initializable;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.util.Preconditions.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Sends requests to the specified servers according to defined
 * {@code RpcStrategy} strategy. Strategies, represented in
 * {@link RpcStrategies} satisfy most cases.
 * <p>
 * Example. Consider a client which sends a {@code Request} and receives a
 * {@code Response} from some {@link RpcServer}. To implement such kind of
 * client its necessary to proceed with following steps:
 * <ul>
 * <li>Create request-response classes for the client</li>
 * <li>Create a request handler for specified types</li>
 * <li>Create {@code RpcClient} and adjust it</li>
 * </ul>
 *
 * @see RpcStrategies
 * @see RpcServer
 */
public final class RpcClient implements IRpcClient, EventloopService, Initializable<RpcClient>, EventloopJmxMBeanEx {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create().withTcpNoDelay(true);
	public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration DEFAULT_RECONNECT_INTERVAL = Duration.ofSeconds(1);
	public static final MemSize DEFAULT_PACKET_SIZE = ChannelBinarySerializer.DEFAULT_INITIAL_BUFFER_SIZE;
	public static final MemSize MAX_PACKET_SIZE = ChannelBinarySerializer.MAX_SIZE;

	private Logger logger = getLogger(getClass());

	private final Eventloop eventloop;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	// SSL
	private SSLContext sslContext;
	private ExecutorService sslExecutor;

	private RpcStrategy strategy = new NoServersStrategy();
	private List<InetSocketAddress> addresses = new ArrayList<>();
	private Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();

	private MemSize defaultPacketSize = DEFAULT_PACKET_SIZE;
	private MemSize maxPacketSize = MAX_PACKET_SIZE;
	private boolean compression = false;
	private Duration autoFlushInterval = Duration.ZERO;

	private List<Class<?>> messageTypes;
	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private long reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL.toMillis();
	private boolean forceStart;

	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	private SerializerBuilder serializerBuilder = SerializerBuilder.create(classLoader);
	private BufferSerializer<RpcMessage> serializer;

	private RpcSender requestSender;

	@Nullable
	private SettablePromise<Void> startPromise;
	@Nullable
	private SettablePromise<Void> stopPromise;
	private boolean running;

	private final RpcClientConnectionPool pool = address -> connections.get(address);

	// jmx
	static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private boolean monitoring = false;
	private final RpcRequestStats generalRequestsStats = RpcRequestStats.create(SMOOTHING_WINDOW);
	private final RpcConnectStats generalConnectsStats = new RpcConnectStats();
	private final Map<Class<?>, RpcRequestStats> requestStatsPerClass = new HashMap<>();
	private final Map<InetSocketAddress, RpcConnectStats> connectsStatsPerAddress = new HashMap<>();
	private final ExceptionStats lastProtocolError = ExceptionStats.create();

	private final JmxInspector statsSocket = new JmxInspector();
//	private final StreamBinarySerializer.JmxInspector statsSerializer = new StreamBinarySerializer.JmxInspector();
//	private final StreamBinaryDeserializer.JmxInspector statsDeserializer = new StreamBinaryDeserializer.JmxInspector();
//	private final StreamLZ4Compressor.JmxInspector statsCompressor = new StreamLZ4Compressor.JmxInspector();
//	private final StreamLZ4Decompressor.JmxInspector statsDecompressor = new StreamLZ4Decompressor.JmxInspector();

	// region builders
	private RpcClient(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static RpcClient create(Eventloop eventloop) {
		return new RpcClient(eventloop);
	}

	public RpcClient withClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		this.serializerBuilder = SerializerBuilder.create(classLoader);
		return this;
	}

	/**
	 * Creates a client that uses provided socket settings.
	 *
	 * @param socketSettings settings for socket
	 * @return the RPC client with specified socket settings
	 */
	public RpcClient withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	/**
	 * Creates a client with capability of specified message types processing.
	 *
	 * @param messageTypes classes of messages processed by a server
	 * @return client instance capable for handling provided message types
	 */
	public RpcClient withMessageTypes(Class<?>... messageTypes) {
		checkNotNull(messageTypes);
		return withMessageTypes(Arrays.asList(messageTypes));
	}

	/**
	 * Creates a client with capability of specified message types processing.
	 *
	 * @param messageTypes classes of messages processed by a server
	 * @return client instance capable for handling provided
	 * message types
	 */
	public RpcClient withMessageTypes(List<Class<?>> messageTypes) {
		checkArgument(new HashSet<>(messageTypes).size() == messageTypes.size(), "Message types must be unique");
		this.messageTypes = messageTypes;
		return this;
	}

	/**
	 * Creates a client with serializer builder. A serializer builder is used
	 * for creating fast serializers at runtime.
	 *
	 * @param serializerBuilder serializer builder, used at runtime
	 * @return the RPC client with provided serializer builder
	 */
	public RpcClient withSerializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return this;
	}

	/**
	 * Creates a client with some strategy. Consider some ready-to-use
	 * strategies from {@link RpcStrategies}.
	 *
	 * @param requestSendingStrategy strategy of sending requests
	 * @return the RPC client, which sends requests according to given strategy
	 */
	public RpcClient withStrategy(RpcStrategy requestSendingStrategy) {
		this.strategy = requestSendingStrategy;
		this.addresses = new ArrayList<>(strategy.getAddresses());

		// jmx
		for (InetSocketAddress address : this.addresses) {
			if (!connectsStatsPerAddress.containsKey(address)) {
				connectsStatsPerAddress.put(address, new RpcConnectStats());
			}
		}

		return this;
	}

	public RpcClient withStreamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		this.defaultPacketSize = defaultPacketSize;
		this.maxPacketSize = maxPacketSize;
		this.compression = compression;
		return this;
	}

	public RpcClient withAutoFlushInterval(Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		return this;
	}

	/**
	 * Waits for a specified time before connecting.
	 *
	 * @param connectTimeout time before connecting
	 * @return the RPC client with connect timeout settings
	 */
	public RpcClient withConnectTimeout(Duration connectTimeout) {
		this.connectTimeoutMillis = connectTimeout.toMillis();
		return this;
	}

	public RpcClient withReconnectInterval(Duration reconnectInterval) {
		this.reconnectIntervalMillis = reconnectInterval.toMillis();
		return this;
	}

	public RpcClient withSslEnabled(SSLContext sslContext, ExecutorService sslExecutor) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		return this;
	}

	public RpcClient withLogger(Logger logger) {
		this.logger = logger;
		return this;
	}

	/**
	 * Starts client in case of absence of connections
	 *
	 * @return the RPC client, which starts regardless of connection
	 * availability
	 */
	public RpcClient withForceStart() {
		this.forceStart = true;
		return this;
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
	public Promise<Void> start() {
		checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		checkNotNull(messageTypes, "Message types must be specified");
		checkState(!running, "Already running");

		SettablePromise<Void> promise = new SettablePromise<>();
		running = true;
		startPromise = promise;
		serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);

		if (forceStart) {
			startPromise.set(null);
			RpcSender sender = strategy.createSender(pool);
			requestSender = sender != null ? sender : new NoSenderAvailable();
			startPromise = null;
		} else {
			if (connectTimeoutMillis != 0) {
				eventloop.delayBackground(connectTimeoutMillis, () -> {
					if (running && startPromise != null) {
						String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
								connectTimeoutMillis / 1000.0);
						startPromise.setException(new InterruptedException(errorMsg));
						running = false;
						startPromise = null;
					}
				});
			}
		}

		for (InetSocketAddress address : addresses) {
			connect(address);
		}

		return promise;
	}

	@Override
	public Promise<Void> stop() {
		if (!running) return Promise.complete();
		checkState(eventloop.inEventloopThread(), "Not in eventloop thread");

		SettablePromise<Void> promise = new SettablePromise<>();

		running = false;
		if (startPromise != null) {
			startPromise.setException(new InterruptedException("Start aborted"));
			startPromise = null;
		}

		if (connections.size() == 0) {
			promise.set(null);
		} else {
			stopPromise = promise;
			for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
				connection.close();
			}
		}

		return promise;
	}

	private void connect(InetSocketAddress address) {
		if (!running) {
			return;
		}

		logger.info("Connecting {}", address);

		AsyncTcpSocketImpl.connect(address, 0, socketSettings)
				.whenResult(asyncTcpSocketImpl -> {
					asyncTcpSocketImpl
							.withInspector(statsSocket);
					AsyncTcpSocket socket = sslContext == null ?
							asyncTcpSocketImpl :
							wrapClientSocket(asyncTcpSocketImpl, sslContext, sslExecutor);
					RpcStream stream = new RpcStream(socket, serializer, defaultPacketSize, maxPacketSize,
							autoFlushInterval, compression, false); // , statsSerializer, statsDeserializer, statsCompressor, statsDecompressor);
					RpcClientConnection connection = new RpcClientConnection(eventloop, this, address, stream);
					stream.setListener(connection);

					addConnection(address, connection);

					// jmx
					generalConnectsStats.successfulConnects++;
					connectsStatsPerAddress.get(address).successfulConnects++;

					logger.info("Connection to {} established", address);
					if (startPromise != null && !(requestSender instanceof NoSenderAvailable)) {
						SettablePromise<Void> startPromise = this.startPromise;
						this.startPromise = null;
						eventloop.postLater(() -> startPromise.set(null));
					}
				})
				.whenException(e -> {
					//jmx
					generalConnectsStats.failedConnects++;
					connectsStatsPerAddress.get(address).failedConnects++;

					if (running) {
						if (logger.isWarnEnabled()) {
							logger.warn("Connection failed, reconnecting to {}: {}", address, e.toString());
						}
						eventloop.delayBackground(reconnectIntervalMillis, () -> {
							if (running) {
								connect(address);
							}
						});
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
		requestSender = sender != null ? sender : new NoSenderAvailable();
	}

	void removeConnection(InetSocketAddress address) {
		if (!connections.containsKey(address)) {
			return;
		}

		logger.info("Connection to {} closed", address);

		connections.remove(address);

		if (stopPromise != null && connections.size() == 0) {
			eventloop.post(() -> {
				stopPromise.set(null);
				stopPromise = null;
			});
		}

		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new NoSenderAvailable();

		// jmx
		generalConnectsStats.closedConnects++;
		connectsStatsPerAddress.get(address).closedConnects++;

		eventloop.delayBackground(reconnectIntervalMillis, () -> {
			if (running) {
				connect(address);
			}
		});
	}

	/**
	 * Sends the request to server, waits the result timeout and handles result with callback
	 *
	 * @param <I>     request class
	 * @param <O>     response class
	 * @param request request for server
	 */
	@Override
	public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
		requestSender.sendRequest(request, timeout, cb);
	}

	public IRpcClient adaptToAnotherEventloop(Eventloop anotherEventloop) {
		if (anotherEventloop == this.eventloop) {
			return this;
		}

		return new IRpcClient() {
			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				eventloop.execute(() ->
						requestSender.sendRequest(request, timeout,
								new Callback<O>() {
									@Override
									public void set(O result) {
										anotherEventloop.execute(() -> cb.set(result));
									}

									@Override
									public void setException(Throwable e) {
										anotherEventloop.execute(() -> cb.setException(e));
									}
								}));
			}

		};
	}

	// visible for testing
	public RpcSender getRequestSender() {
		return requestSender;
	}

	@Override
	public String toString() {
		return "RpcClient{" +
				"connections=" + connections +
				'}';
	}

	private final class NoSenderAvailable implements RpcSender {
		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			eventloop.post(() -> cb.setException(NO_SENDER_AVAILABLE_EXCEPTION));
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

	@JmxAttribute(name = "requests", extraSubAttributes = "totalRequests")
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

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActiveConnections() {
		return connections.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActiveRequests() {
		int count = 0;
		for (RpcClientConnection connection : connections.values()) {
			count += connection.getActiveRequests();
		}
		return count;
	}

	@JmxAttribute(description = "exception that occurred because of protocol error " +
			"(serialization, deserialization, compression, decompression, etc)")
	public ExceptionStats getLastProtocolError() {
		return lastProtocolError;
	}

	@JmxAttribute
	public JmxInspector getStatsSocket() {
		return statsSocket;
	}

//	@JmxAttribute
//	public StreamBinarySerializer.JmxInspector getStatsSerializer() {
//		return statsSerializer;
//	}
//
//	@JmxAttribute
//	public StreamBinaryDeserializer.JmxInspector getStatsDeserializer() {
//		return statsDeserializer;
//	}
//
//	@JmxAttribute
//	public StreamLZ4Compressor.JmxInspector getStatsCompressor() {
//		return compression ? statsCompressor : null;
//	}
//
//	@JmxAttribute
//	public StreamLZ4Decompressor.JmxInspector getStatsDecompressor() {
//		return compression ? statsDecompressor : null;
//	}

	RpcRequestStats ensureRequestStatsPerClass(Class<?> requestClass) {
		if (!requestStatsPerClass.containsKey(requestClass)) {
			requestStatsPerClass.put(requestClass, RpcRequestStats.create(SMOOTHING_WINDOW));
		}
		return requestStatsPerClass.get(requestClass);
	}
}
