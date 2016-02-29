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

import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.*;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.server.RpcServerConnection.StatusListener;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import org.slf4j.Logger;

import java.nio.channels.SocketChannel;
import java.util.*;

import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

public final class RpcServer extends AbstractServer<RpcServer> {
	private Logger logger = getLogger(RpcServer.class);
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = new ServerSocketSettings(16384);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);

	private final Map<Class<?>, RpcRequestHandler<Object>> handlers = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private SerializerBuilder serializerBuilder;
	private final Set<Class<?>> messageTypes = new LinkedHashSet<>();

	private final Map<SocketChannel, RpcServerConnection> connections = new HashMap<>();

	// jmx
	private CountStats connectionsCount = new CountStats();
	private boolean monitoring;

	private RpcServer(Eventloop eventloop) {
		super(eventloop);
		serverSocketSettings(DEFAULT_SERVER_SOCKET_SETTINGS);
		socketSettings(DEFAULT_SOCKET_SETTINGS);
	}

	public static RpcServer create(Eventloop eventloop) {
		return new RpcServer(eventloop);
	}

	public RpcServer messageTypes(Class<?>... messageTypes) {
		return messageTypes(Arrays.asList(messageTypes));
	}

	public RpcServer messageTypes(List<Class<?>> messageTypes) {
		this.messageTypes.addAll(messageTypes);
		return this;
	}

	public RpcServer serializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return this;
	}

	public RpcServer logger(Logger logger) {
		this.logger = checkNotNull(logger);
		return this;
	}

	public RpcServer protocol(RpcProtocolFactory protocolFactory) {
		this.protocolFactory = protocolFactory;
		return this;
	}

	@SuppressWarnings("unchecked")
	public <T> RpcServer on(Class<T> requestClass, RpcRequestHandler<T> handler) {
		handlers.put(requestClass, (RpcRequestHandler<Object>) handler);
		return this;
	}

	private BufferSerializer<RpcMessage> createSerializer() {
		SerializerBuilder serializerBuilder = this.serializerBuilder != null ?
				this.serializerBuilder :
				SerializerBuilder.newDefaultInstance(ClassLoader.getSystemClassLoader());
		serializerBuilder.setExtraSubclasses("extraRpcMessageData", messageTypes);
		return serializerBuilder.create(RpcMessage.class);
	}

	@Override
	protected SocketConnection createConnection(final SocketChannel socketChannel) {
		StatusListener statusListener = new StatusListener() {
			@Override
			public void onOpen(RpcServerConnection connection) {
				connections.put(socketChannel, connection);

				// jmx
				connectionsCount.setCount(connections.size());
			}

			@Override
			public void onClosed() {
				connections.remove(socketChannel);

				// jmx
				connectionsCount.setCount(connections.size());
			}
		};
		BufferSerializer<RpcMessage> messageSerializer = createSerializer();
		RpcServerConnection serverConnection = new RpcServerConnection(eventloop, socketChannel,
				messageSerializer, handlers, protocolFactory, statusListener);
		return serverConnection.getSocketConnection();
	}

	@Override
	protected void onClose() {
		closeConnections();
	}

	private void closeConnections() {
		for (final RpcServerConnection connection : new ArrayList<>(connections.values())) {
			connection.close();
		}
	}

	public void add(SocketChannel socketChannel, RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client connected on {}", socketChannel);
		connections.put(socketChannel, connection);

		// jmx
		connectionsCount.setCount(connections.size());
	}

	public void remove(SocketChannel socketChannel) {
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", socketChannel);
		connections.remove(socketChannel);

		// jmx
		connectionsCount.setCount(connections.size());
	}

	// JMX
	@JmxOperation
	public void startMonitoring() {
		monitoring = true;
		for (RpcServerConnection connection : connections.values()) {
			connection.startMonitoring();
		}
	}

	@JmxOperation
	public void stopMonitoring() {
		monitoring = false;
		for (RpcServerConnection connection : connections.values()) {
			connection.stopMonitoring();
		}
	}

	@JmxAttribute
	public boolean getMonitoring() {
		return monitoring;
	}

	@JmxAttribute(name = "currentConnections")
	public CountStats getConnectionsCount() {
		return connectionsCount;
	}

	@JmxAttribute
	public List<RpcServerConnection> getConnections() {
		return new ArrayList<>(connections.values());
	}

	@JmxAttribute
	public EventStats getRequests() {
		EventStats totalRequests = new EventStats();
		for (RpcServerConnection connection : connections.values()) {
			totalRequests.add(connection.getSuccessfulResponses());
			totalRequests.add(connection.getErrorResponses());
		}
		return totalRequests;
	}

	@JmxAttribute
	public EventStats getProcessingErrors() {
		EventStats errors = new EventStats();
		for (RpcServerConnection connection : connections.values()) {
			errors.add(connection.getErrorResponses());
		}
		return errors;
	}

	@JmxAttribute
	public ValueStats getRequestHandlingTime() {
		ValueStats requestHandlingTime = new ValueStats();
		for (RpcServerConnection connection : connections.values()) {
			requestHandlingTime.add(connection.getRequestHandlingTime());
		}
		return requestHandlingTime;
	}
}

