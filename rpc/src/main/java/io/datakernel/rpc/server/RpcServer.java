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
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import org.slf4j.Logger;

import java.util.*;

import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

public final class RpcServer extends AbstractServer<RpcServer> {
	private Logger logger = getLogger(RpcServer.class);
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = new ServerSocketSettings(16384);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);

	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private SerializerBuilder serializerBuilder;
	private final Set<Class<?>> messageTypes = new LinkedHashSet<>();

	private final List<RpcServerConnection> connections = new ArrayList<>();

	private BufferSerializer<RpcMessage> serializer;

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
	public <I> RpcServer on(Class<I> requestClass, RpcRequestHandler<I, ?> handler) {
		handlers.put(requestClass, handler);
		return this;
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

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		BufferSerializer<RpcMessage> messageSerializer = getSerializer();
		RpcServerConnection connection = new RpcServerConnection(eventloop, this, asyncTcpSocket,
				messageSerializer, handlers, protocolFactory);
		add(connection);
		return connection.getSocketConnection();
	}

	@Override
	protected void onClose() {
		for (final RpcServerConnection connection : new ArrayList<>(connections)) {
			connection.close();
		}
	}

	public void add(RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client connected on {}", connection);
		connections.add(connection);

		// jmx
		connectionsCount.setCount(connections.size());
	}

	public void remove(RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", connection);
		connections.remove(connection);

		// jmx
		connectionsCount.setCount(connections.size());
	}

	// JMX
	@JmxOperation
	public void startMonitoring() {
		monitoring = true;
		for (RpcServerConnection connection : connections) {
			connection.startMonitoring();
		}
	}

	@JmxOperation
	public void stopMonitoring() {
		monitoring = false;
		for (RpcServerConnection connection : connections) {
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
		return new ArrayList<>(connections);
	}

	@JmxAttribute
	public EventStats getRequests() {
		EventStats totalRequests = new EventStats();
		for (RpcServerConnection connection : connections) {
			totalRequests.add(connection.getSuccessfulResponses());
			totalRequests.add(connection.getErrorResponses());
		}
		return totalRequests;
	}

	@JmxAttribute
	public EventStats getProcessingErrors() {
		EventStats errors = new EventStats();
		for (RpcServerConnection connection : connections) {
			errors.add(connection.getErrorResponses());
		}
		return errors;
	}

	@JmxAttribute
	public ValueStats getRequestHandlingTime() {
		ValueStats requestHandlingTime = new ValueStats();
		for (RpcServerConnection connection : connections) {
			requestHandlingTime.add(connection.getRequestHandlingTime());
		}
		return requestHandlingTime;
	}
}

