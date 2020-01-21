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

import io.datakernel.common.MemSize;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.jmx.ExceptionStats;
import io.datakernel.eventloop.jmx.ValueStats;
import io.datakernel.eventloop.net.ServerSocketSettings;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.jmx.api.JmxReducers.JmxReducerSum;
import io.datakernel.net.AbstractServer;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.promise.SettablePromise;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcStream;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;
import java.time.Duration;
import java.util.*;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.Preconditions.checkState;
import static java.util.Arrays.asList;

/**
 * An RPC server that works asynchronously. This server uses fast serializers
 * and custom optimized communication protocol, improving application
 * performance.
 * <p>
 * In order to set up a server it's mandatory to create it using
 * {@link #create(Eventloop)}, indicate a types of messages, and specify
 * an appropriate {@link RpcRequestHandler request handlers} for that types.
 * <p>
 * There are two ways of starting a server:
 * <ul>
 * <li>Manually: set up the server and call {@code listen()}</li>
 * <li>Create a module for your RPC server and pass it to a {@code Launcher}
 * along with {@code ServiceGraphModule}.</li>
 * </ul>
 * <p>
 * Example. Here are the steps, intended to supplement the example, listed in
 * {@link RpcClient}:
 * <ul>
 * <li>Create a {@code RequestHandler} for {@code RequestClass} and
 * {@code ResponseClass}</li>
 * <li>Create an {@code RpcServer}</li>
 * <li>Run the server</li>
 * </ul>
 *
 * @see RpcRequestHandler
 * @see RpcClient
 */
public final class RpcServer extends AbstractServer<RpcServer> {
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = ServerSocketSettings.create(16384);

	public static final MemSize DEFAULT_INITIAL_BUFFER_SIZE = ChannelSerializer.DEFAULT_INITIAL_BUFFER_SIZE;
	public static final MemSize DEFAULT_MAX_MESSAGE_SIZE = ChannelSerializer.MAX_SIZE;

	private MemSize initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;
	private MemSize maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
	private boolean compression = false;
	private Duration autoFlushInterval = Duration.ZERO;

	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers = new LinkedHashMap<>();
	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	private SerializerBuilder serializerBuilder = SerializerBuilder.create(classLoader);
	private List<Class<?>> messageTypes;

	private final List<RpcServerConnection> connections = new ArrayList<>();

	private BinarySerializer<RpcMessage> serializer;

	private SettablePromise<Void> closeCallback;

	// region JMX vars
	static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private final EventStats totalConnects = EventStats.create(SMOOTHING_WINDOW);
	private final Map<InetAddress, EventStats> connectsPerAddress = new HashMap<>();
	private final EventStats successfulRequests = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats failedRequests = EventStats.create(SMOOTHING_WINDOW);
	private final ValueStats requestHandlingTime = ValueStats.create(SMOOTHING_WINDOW).withUnit("milliseconds");
	private final ExceptionStats lastRequestHandlingException = ExceptionStats.create();
	private final ExceptionStats lastProtocolError = ExceptionStats.create();
	private boolean monitoring;
	// endregion

//	private final StreamBinarySerializer.JmxInspector statsSerializer = new StreamBinarySerializer.JmxInspector();
//	private final StreamBinaryDeserializer.JmxInspector statsDeserializer = new StreamBinaryDeserializer.JmxInspector();
//	private final StreamLZ4Compressor.JmxInspector statsCompressor = new StreamLZ4Compressor.JmxInspector();
//	private final StreamLZ4Decompressor.JmxInspector statsDecompressor = new StreamLZ4Decompressor.JmxInspector();

	// region builders
	private RpcServer(Eventloop eventloop) {
		super(eventloop);
	}

	public static RpcServer create(Eventloop eventloop) {
		return new RpcServer(eventloop)
				.withServerSocketSettings(DEFAULT_SERVER_SOCKET_SETTINGS)
				.withSocketSettings(DEFAULT_SOCKET_SETTINGS);
	}

	public RpcServer withClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		this.serializerBuilder = SerializerBuilder.create(classLoader);
		return this;
	}

	/**
	 * Creates a server, capable of specified message types processing.
	 *
	 * @param messageTypes classes of messages processed by a server
	 * @return server instance capable for handling provided message types
	 */
	public RpcServer withMessageTypes(@NotNull Class<?>... messageTypes) {
		return withMessageTypes(asList(messageTypes));
	}

	/**
	 * Creates a server, capable of specified message types processing.
	 *
	 * @param messageTypes a list of message types processed by a server
	 * @return server instance capable for handling provided message types
	 */
	public RpcServer withMessageTypes(@NotNull List<Class<?>> messageTypes) {
		checkArgument(new HashSet<>(messageTypes).size() == messageTypes.size(), "Message types must be unique");
		this.messageTypes = messageTypes;
		return this;
	}

	public RpcServer withSerializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return this;
	}

	public RpcServer withStreamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		this.initialBufferSize = defaultPacketSize;
		this.maxMessageSize = maxPacketSize;
		this.compression = compression;
		return this;
	}

	public RpcServer withAutoFlushInterval(Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		return this;
	}

	/**
	 * Adds a handler for a specified request-response pair.
	 *
	 * @param requestClass  a class representing a request structure
	 * @param responseClass a class representing a response structure
	 * @param handler       a class containing logic of request processing and
	 *                      creating a response
	 * @param <I>           class of request
	 * @param <O>           class of response
	 * @return server instance capable for handling requests of concrete types
	 */
	public <I, O> RpcServer withHandler(Class<I> requestClass, Class<O> responseClass, RpcRequestHandler<I, O> handler) {
		handlers.put(requestClass, handler);
		return this;
	}

	// endregion

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		RpcStream stream = new RpcStream(socket, serializer, initialBufferSize, maxMessageSize,
				autoFlushInterval, compression, true); // , statsSerializer, statsDeserializer, statsCompressor, statsDecompressor);
		RpcServerConnection connection = new RpcServerConnection(this, remoteAddress, handlers, stream);
		stream.setListener(connection);
		add(connection);

		// jmx
		ensureConnectStats(remoteAddress).recordEvent();
		totalConnects.recordEvent();
	}

	@Override
	protected void onListen() {
		checkState(messageTypes != null, "Message types must be specified");
		serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);
	}

	@Override
	protected void onClose(SettablePromise<Void> cb) {
		if (connections.size() == 0) {
			logger.info("RpcServer is closing. Active connections count: 0.");
			cb.set(null);
		} else {
			logger.info("RpcServer is closing. Active connections count: " + connections.size());
			for (RpcServerConnection connection : new ArrayList<>(connections)) {
				connection.shutdown();
			}
			closeCallback = cb;
		}
	}

	void add(RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client connected on {}", connection);

		if (monitoring) {
			connection.startMonitoring();
		}

		connections.add(connection);
	}

	boolean remove(RpcServerConnection connection) {
		if (!connections.remove(connection)) {
			return false;
		}
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", connection);

		if (closeCallback != null) {
			logger.info("RpcServer is closing. One more connection was closed. " +
					"Active connections count: " + connections.size());

			if (connections.size() == 0) {
				closeCallback.set(null);
			}
		}
		return true;
	}

	// region JMX
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled) ]")
	public void startMonitoring() {
		monitoring = true;
		for (RpcServerConnection connection : connections) {
			connection.startMonitoring();
		}
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled) ]")
	public void stopMonitoring() {
		monitoring = false;
		for (RpcServerConnection connection : connections) {
			connection.stopMonitoring();
		}
	}

	@JmxAttribute(description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled)")
	public boolean isMonitoring() {
		return monitoring;
	}

	@JmxAttribute(description = "current number of connections", reducer = JmxReducerSum.class)
	public int getConnectionsCount() {
		return connections.size();
	}

	@JmxAttribute
	public EventStats getTotalConnects() {
		return totalConnects;
	}

	//	FIXME (vmykhalko) @JmxAttribute(description = "number of connects/reconnects per client address")
	public Map<InetAddress, EventStats> getConnectsPerAddress() {
		return connectsPerAddress;
	}

	private EventStats ensureConnectStats(InetAddress address) {
		EventStats stats = connectsPerAddress.get(address);
		if (stats == null) {
			stats = EventStats.create(SMOOTHING_WINDOW);
			connectsPerAddress.put(address, stats);
		}
		return stats;
	}

	@JmxAttribute(description = "detailed information about connections")
	public List<RpcServerConnection> getConnections() {
		return connections;
	}

	@JmxAttribute(extraSubAttributes = "totalCount", description = "number of requests which were processed correctly")
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute(extraSubAttributes = "totalCount", description = "request with error responses (number of requests which were handled with error)")
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute(description = "time for handling one request in milliseconds (both successful and failed)")
	public ValueStats getRequestHandlingTime() {
		return requestHandlingTime;
	}

	@JmxAttribute(description = "exception that occurred because of business logic error " +
			"(in RpcRequestHandler implementation)")
	public ExceptionStats getLastRequestHandlingException() {
		return lastRequestHandlingException;
	}

	@JmxAttribute(description = "exception that occurred because of protocol error " +
			"(serialization, deserialization, compression, decompression, etc)")
	public ExceptionStats getLastProtocolError() {
		return lastProtocolError;
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
	// endregion
}

