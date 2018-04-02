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

import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
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
import io.datakernel.rpc.client.sender.RpcNoSenderException;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategies;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcStream;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.util.Initializable;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
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
 * <pre><code>
 * //create a Request and Response classes
 * public class RequestClass {
 * 	private final String info;
 *
 * 	public RequestClass(@Deserialize("info") String info) {
 * 		this.info = info;
 *    }
 *
 *    {@literal @}Serialize(order = 0)
 * 	public String getInfo() {
 * 		return info;
 *        }
 * }
 *
 * public class ResponseClass {
 * 	private final int count;
 *
 * 	public ResponseClass(@Deserialize("count") int count) {
 * 		this.count = count;
 *    }
 *
 *    {@literal @}Serialize(order = 0)
 * 	public int getCount() {
 * 		return count;
 *    }
 * }</code></pre>
 * <p>
 * The last step is to create an {@code RpcClient} itself:
 * <pre><code>
 * //create eventloop
 * Eventloop eventloop = Eventloop.create();
 * InetSocketAddress address = new InetSocketAddress("localhost", 40000);
 * //create client with eventloop
 * RpcClient client = RpcClient.create(eventloop)
 * 				.withMessageTypes(RequestClass.class, ResponseClass.class)
 * 				.withStrategy(RpcStrategies.server(address));
 * </code></pre>
 * Finally, make the client to send a request after start:
 * <code><pre>client.start(new CompletionCallback() {
 *    {@literal @}Override
 * 	public void onComplete() {
 * 		client.sendRequest(new RequestClass(info), 1000,
 * 			new ResultCallback&lt;ResponseClass&gt;() {
 *            {@literal @}Override
 * 			public void onResult(ResponseClass result) {
 * 				System.out.println("Request info length: " + result.getCount());
 *            }
 * <p>
 *            {@literal @}Override
 * 			public void onException(Exception exception) {
 * 				System.err.println("Got exception: " + exception);
 *            }
 *        });
 *    }
 * <p>
 *    {@literal @}Override
 * 	public void onException(Exception exception) {
 * 		System.err.println("Could not start client: " + exception);
 *    }
 * });
 * </pre></code>
 *
 * @see RpcStrategies
 * @see RpcServer
 */
public final class RpcClient implements IRpcClient, EventloopService, Initializable<RpcClient>, EventloopJmxMBeanEx {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create().withTcpNoDelay(true);
	public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration DEFAULT_RECONNECT_INTERVAL = Duration.ofSeconds(1);
	public static final MemSize DEFAULT_PACKET_SIZE = StreamBinarySerializer.DEFAULT_BUFFER_SIZE;
	public static final MemSize MAX_PACKET_SIZE = StreamBinarySerializer.MAX_SIZE;

	private Logger logger = getLogger(this.getClass());

	private final Eventloop eventloop;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	// SSL
	private SSLContext sslContext;
	private ExecutorService sslExecutor;

	private RpcStrategy strategy = new NoServersStrategy();
	private List<InetSocketAddress> addresses = new ArrayList<>();
	private Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();

	private int defaultPacketSize = DEFAULT_PACKET_SIZE.toInt();
	private int maxPacketSize = MAX_PACKET_SIZE.toInt();
	private boolean compression = false;
	private int flushDelayMillis = 0;

	private List<Class<?>> messageTypes;
	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private long reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL.toMillis();
	private boolean forceStart;

	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	private SerializerBuilder serializerBuilder = SerializerBuilder.create(classLoader);
	private BufferSerializer<RpcMessage> serializer;

	private RpcSender requestSender;

	private SettableStage<Void> startStage;
	private SettableStage<Void> stopStage;
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

	private final AsyncTcpSocketImpl.JmxInspector statsSocket = new AsyncTcpSocketImpl.JmxInspector();
//	private final StreamBinarySerializer.JmxInspector statsSerializer = new StreamBinarySerializer.JmxInspector();
//	private final StreamBinaryDeserializer.JmxInspector statsDeserializer = new StreamBinaryDeserializer.JmxInspector();
//	private final StreamLZ4Compressor.JmxInspector statsCompressor = new StreamLZ4Compressor.JmxInspector();
//	private final StreamLZ4Decompressor.JmxInspector statsDecompressor = new StreamLZ4Decompressor.JmxInspector();

	// region builders
	private RpcClient(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@SuppressWarnings("ConstantConditions")
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

	public RpcClient withStreamProtocol(int defaultPacketSize, int maxPacketSize, boolean compression) {
		this.defaultPacketSize = defaultPacketSize;
		this.maxPacketSize = maxPacketSize;
		this.compression = compression;
		return this;
	}

	public RpcClient withStreamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		return withStreamProtocol(defaultPacketSize.toInt(), maxPacketSize.toInt(), compression);
	}

	public RpcClient withFlushDelay(Duration flushDelay) {
		return withFlushDelay((int) flushDelay.toMillis());
	}

	public RpcClient withFlushDelay(int flushDelayMillis) {
		this.flushDelayMillis = flushDelayMillis;
		return this;
	}

	public RpcClient withConnectTimeout(Duration connectTimeout) {
		return withConnectTimeout(connectTimeout.toMillis());
	}

	/**
	 * Waits for a specified time before connecting.
	 *
	 * @param connectTimeoutMillis time before connecting
	 * @return the RPC client with connect timeout settings
	 */
	public RpcClient withConnectTimeout(long connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
		return this;
	}

	public RpcClient withReconnectInterval(Duration reconnectInterval) {
		return withReconnectInterval(reconnectInterval.toMillis());
	}

	public RpcClient withReconnectInterval(long reconnectIntervalMillis) {
		this.reconnectIntervalMillis = reconnectIntervalMillis;
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
	public Stage<Void> start() {
		checkState(eventloop.inEventloopThread());
		checkState(messageTypes != null, "Message types must be specified");
		checkState(!running);

		SettableStage<Void> stage = SettableStage.create();
		running = true;
		startStage = stage;
		serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);

		if (forceStart) {
			startStage.set(null);
			RpcSender sender = strategy.createSender(pool);
			requestSender = sender != null ? sender : new NoSenderAvailable();
			startStage = null;
		} else {
			if (connectTimeoutMillis != 0) {
				eventloop.delayBackground(connectTimeoutMillis, () -> {
					if (running && this.startStage != null) {
						String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
								connectTimeoutMillis / 1000.0);
						this.startStage.setException(new InterruptedException(errorMsg));
						running = false;
						this.startStage = null;
					}
				});
			}
		}

		for (InetSocketAddress address : addresses) {
			connect(address);
		}

		return stage;
	}

	public Future<Void> startFuture() {
		return eventloop.submit(this::start);
	}

	@Override
	public Stage<Void> stop() {
		checkState(eventloop.inEventloopThread());
		checkState(running);

		SettableStage<Void> stage = SettableStage.create();

		running = false;
		if (startStage != null) {
			startStage.setException(new InterruptedException("Start aborted"));
			startStage = null;
		}

		if (connections.size() == 0) {
			stage.set(null);
		} else {
			stopStage = stage;
			for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
				connection.close();
			}
		}

		return stage;
	}

	public Future<Void> stopFuture() {
		return eventloop.submit(this::stop);
	}

	private void connect(InetSocketAddress address) {
		if (!running) {
			return;
		}

		logger.info("Connecting {}", address);

		eventloop.connect(address, 0).whenComplete((socketChannel, throwable) -> {
			if (throwable == null) {
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings)
						.withInspector(statsSocket);
				AsyncTcpSocket asyncTcpSocket = sslContext != null ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
				RpcStream stream = new RpcStream(asyncTcpSocket, serializer, defaultPacketSize, maxPacketSize,
						flushDelayMillis, compression, false); // , statsSerializer, statsDeserializer, statsCompressor, statsDecompressor);
				RpcClientConnection connection = new RpcClientConnection(eventloop, RpcClient.this, address, stream);
				stream.setListener(connection);
				asyncTcpSocket.setEventHandler(stream.getSocketEventHandler());
				asyncTcpSocketImpl.register();

				addConnection(address, connection);

				// jmx
				generalConnectsStats.successfulConnects++;
				connectsStatsPerAddress.get(address).successfulConnects++;

				logger.info("Connection to {} established", address);
				if (startStage != null && !(requestSender instanceof NoSenderAvailable)) {
					SettableStage<Void> startStage = this.startStage;
					this.startStage = null;
					eventloop.postLater(() -> startStage.set(null));
				}
			} else {
				//jmx
				generalConnectsStats.failedConnects++;
				connectsStatsPerAddress.get(address).failedConnects++;

				if (running) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, throwable.toString());
					}
					eventloop.delayBackground(reconnectIntervalMillis, () -> {
						if (running) {
							connect(address);
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
		requestSender = sender != null ? sender : new NoSenderAvailable();
	}

	void removeConnection(InetSocketAddress address) {
		if (!connections.containsKey(address)) {
			return;
		}

		logger.info("Connection to {} closed", address);

		connections.remove(address);

		if (stopStage != null && connections.size() == 0) {
			eventloop.post(() -> {
				stopStage.set(null);
				stopStage = null;
			});
		}

		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new NoSenderAvailable();

		// jmx
		generalConnectsStats.closedConnects++;
		connectsStatsPerAddress.get(address).closedConnects++;

		eventloop.delayBackground(reconnectIntervalMillis, new Runnable() {
			@Override
			public void run() {
				if (running) {
					connect(address);
				}
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
	public <I, O> void sendRequest(I request, int timeout, Callback<O> callback) {
		requestSender.sendRequest(request, timeout, callback);
	}

	public IRpcClient adaptToAnotherEventloop(Eventloop anotherEventloop) {
		if (anotherEventloop == this.eventloop) {
			return this;
		}

		return new IRpcClient() {
			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				RpcClient.this.eventloop.execute(() ->
						RpcClient.this.requestSender.sendRequest(request, timeout,
								new Callback<O>() {
									@Override
									public void set(O result) {
										anotherEventloop.execute(() -> cb.set(result));
									}

									@Override
									public void setException(Throwable throwable) {
										anotherEventloop.execute(() -> cb.setException(throwable));
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
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private final RpcNoSenderException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderException("No senders available");

		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			eventloop.post(() ->
					cb.setException(NO_SENDER_AVAILABLE_EXCEPTION));
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
	public AsyncTcpSocketImpl.JmxInspector getStatsSocket() {
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
