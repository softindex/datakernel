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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcStream;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import io.datakernel.util.MemSize;

import java.net.InetAddress;
import java.util.*;

import static io.datakernel.util.Preconditions.*;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.Arrays.asList;

public final class RpcServer extends AbstractServer<RpcServer> {
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = ServerSocketSettings.create(16384);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create().withTcpNoDelay(true);

	public static final MemSize DEFAULT_PACKET_SIZE = StreamBinarySerializer.DEFAULT_BUFFER_SIZE;
	public static final MemSize MAX_PACKET_SIZE = StreamBinarySerializer.MAX_SIZE;

	private int defaultPacketSize = (int) DEFAULT_PACKET_SIZE.get();
	private int maxPacketSize = (int) MAX_PACKET_SIZE.get();
	private boolean compression = false;

	private Map<Class<?>, RpcRequestHandler<?, ?>> handlers = new LinkedHashMap<>();
	private SerializerBuilder serializerBuilder = SerializerBuilder.create(getSystemClassLoader());
	private List<Class<?>> messageTypes;

	private final List<RpcServerConnection> connections = new ArrayList<>();

	private BufferSerializer<RpcMessage> serializer;

	private CompletionCallback closeCallback;

	// jmx
	static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;
	private EventStats totalConnects = EventStats.create(SMOOTHING_WINDOW);
	private Map<InetAddress, EventStats> connectsPerAddress = new HashMap<>();
	private EventStats successfulRequests = EventStats.create(SMOOTHING_WINDOW);
	private EventStats failedRequests = EventStats.create(SMOOTHING_WINDOW);
	private ValueStats requestHandlingTime = ValueStats.create(SMOOTHING_WINDOW);
	private ExceptionStats lastRequestHandlingException = ExceptionStats.create();
	private ExceptionStats lastProtocolError = ExceptionStats.create();
	private boolean monitoring;

	private final AsyncTcpSocketImpl.JmxInspector statsSocket = new AsyncTcpSocketImpl.JmxInspector();
	private final StreamBinarySerializer.JmxInspector statsSerializer = new StreamBinarySerializer.JmxInspector();
	private final StreamBinaryDeserializer.JmxInspector statsDeserializer = new StreamBinaryDeserializer.JmxInspector();
	private final StreamLZ4Compressor.JmxInspector statsCompressor = new StreamLZ4Compressor.JmxInspector();
	private final StreamLZ4Decompressor.JmxInspector statsDecompressor = new StreamLZ4Decompressor.JmxInspector();

	// region builders
	private RpcServer(Eventloop eventloop) {
		super(eventloop);
	}

	public static RpcServer create(Eventloop eventloop) {
		return new RpcServer(eventloop)
				.withServerSocketSettings(DEFAULT_SERVER_SOCKET_SETTINGS)
				.withSocketSettings(DEFAULT_SOCKET_SETTINGS);
	}

	public RpcServer withMessageTypes(Class<?>... messageTypes) {
		checkNotNull(messageTypes);
		return withMessageTypes(asList(messageTypes));
	}

	public RpcServer withMessageTypes(List<Class<?>> messageTypes) {
		checkArgument(new HashSet<>(messageTypes).size() == messageTypes.size(), "Message types must be unique");
		this.messageTypes = messageTypes;
		return self();
	}

	public RpcServer withSerializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return self();
	}

	public RpcServer withStreamProtocol(int defaultPacketSize, int maxPacketSize, boolean compression) {
		this.defaultPacketSize = defaultPacketSize;
		this.maxPacketSize = maxPacketSize;
		this.compression = compression;
		return self();
	}

	public RpcServer withStreamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		return withStreamProtocol((int) defaultPacketSize.get(), (int) maxPacketSize.get(), compression);
	}

	@SuppressWarnings("unchecked")
	public <I, O> RpcServer withHandler(Class<I> requestClass, Class<O> responseClass, RpcRequestHandler<I, O> handler) {
		handlers.put(requestClass, handler);
		return this;
	}
	// endregion

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		RpcStream stream = new RpcStream(eventloop, asyncTcpSocket, serializer, defaultPacketSize, maxPacketSize, compression, true,
				statsSerializer, statsDeserializer, statsCompressor, statsDecompressor);
		RpcServerConnection connection = new RpcServerConnection(eventloop, this,
				asyncTcpSocket.getRemoteSocketAddress(), handlers, stream);
		stream.setListener(connection);
		add(connection);

		// jmx
		ensureConnectStats(asyncTcpSocket.getRemoteSocketAddress().getAddress()).recordEvent();
		totalConnects.recordEvent();

		return stream.getSocketEventHandler();
	}

	@Override
	protected void onListen() {
		checkState(messageTypes != null, "Message types must be specified");
		serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);
	}

	@Override
	protected void onClose(final CompletionCallback completionCallback) {
		if (connections.size() == 0) {
			logger.info("RpcServer is closing. Active connections count: 0.");
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					completionCallback.setComplete();
				}
			});
		} else {
			logger.info("RpcServer is closing. Active connections count: " + connections.size());
			for (final RpcServerConnection connection : new ArrayList<>(connections)) {
				connection.close();
			}
			closeCallback = completionCallback;
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

	void remove(RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", connection);
		connections.remove(connection);

		if (closeCallback != null) {
			logger.info("RpcServer is closing. One more connection was closed. " +
					"Active connections count: " + connections.size());

			if (connections.size() == 0) {
				closeCallback.setComplete();
			}
		}
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

	@JmxAttribute(description = "number of requests which were processed correctly")
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute(description = "request with error responses (number of requests which were handled with error)")
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

	@JmxAttribute
	public AsyncTcpSocketImpl.JmxInspector getStatsSocket() {
		return statsSocket;
	}

	@JmxAttribute
	public StreamBinarySerializer.JmxInspector getStatsSerializer() {
		return statsSerializer;
	}

	@JmxAttribute
	public StreamBinaryDeserializer.JmxInspector getStatsDeserializer() {
		return statsDeserializer;
	}

	@JmxAttribute
	public StreamLZ4Compressor.JmxInspector getStatsCompressor() {
		return compression ? statsCompressor : null;
	}

	@JmxAttribute
	public StreamLZ4Decompressor.JmxInspector getStatsDecompressor() {
		return compression ? statsDecompressor : null;
	}
	// endregion
}

