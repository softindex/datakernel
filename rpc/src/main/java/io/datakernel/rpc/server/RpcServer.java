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

import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.protocol.RpcSerializer;
import io.datakernel.rpc.server.RpcServerConnection.StatusListener;
import io.datakernel.serializer.BufferSerializer;
import org.slf4j.Logger;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

public final class RpcServer extends AbstractNioServer<RpcServer> implements RpcServerMBean {
	private Logger logger = getLogger(RpcServer.class);
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = new ServerSocketSettings(16384);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);

	private final Map<Class<?>, RpcRequestHandler<Object>> handlers = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private final RpcSerializer serializer;

	private final Map<SocketChannel, RpcServerConnection> connections = new HashMap<>();

	private RpcServer(NioEventloop eventloop, RpcSerializer serializer) {
		super(eventloop);
		this.serializer = serializer;
		serverSocketSettings(DEFAULT_SERVER_SOCKET_SETTINGS);
		socketSettings(DEFAULT_SOCKET_SETTINGS);
	}

	public static RpcServer create(NioEventloop eventloop, RpcSerializer serializerFactory) {
		return new RpcServer(eventloop, serializerFactory);
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

	@Override
	protected SocketConnection createConnection(final SocketChannel socketChannel) {
		StatusListener statusListener = new StatusListener() {
			@Override
			public void onOpen(RpcServerConnection connection) {
				connections.put(socketChannel, connection);
			}

			@Override
			public void onClosed() {
				connections.remove(socketChannel);
			}
		};
		BufferSerializer<RpcMessage> messageSerializer = serializer.createSerializer();
		RpcServerConnection serverConnection = new RpcServerConnection(eventloop, socketChannel,
				messageSerializer, handlers, protocolFactory, statusListener);
		return serverConnection.getSocketConnection();
	}

	@Override
	protected void onClose() {
		closeConnections();
	}

	private void closeConnections() {
		for (RpcServerConnection connection : connections.values()) {
			connection.close();
		}
	}

	// JMX
	private boolean monitoring;

	public void add(SocketChannel socketChannel, RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client connected on {}", socketChannel);
		connections.put(socketChannel, connection);
	}

	public void remove(SocketChannel socketChannel) {
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", socketChannel);
		connections.remove(socketChannel);
	}

	@Override
	public void startMonitoring() {
		monitoring = true;
		for (RpcServerConnection connection : connections.values()) {
			connection.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (RpcServerConnection connection : connections.values()) {
			connection.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		for (RpcServerConnection connection : connections.values()) {
			connection.reset();
		}
	}

	@Override
	public int getConnectionsCount() {
		return connections.size();
	}

	@Override
	public CompositeData[] getConnections() throws OpenDataException {
		List<CompositeData> compositeData = new ArrayList<>();
		for (SocketChannel socketChannel : connections.keySet()) {
			RpcServerConnection connection = connections.get(socketChannel);
			CompositeData lastResponseException = connection.getLastResponseException();
			CompositeData lastInternalException = connection.getLastInternalException();
			CompositeData connectionDetails = connection.getConnectionDetails();
			compositeData.add(CompositeDataBuilder.builder("Rpc connections", "Rpc connections status")
					.add("SocketInfo", SimpleType.STRING, socketChannel.toString())
					.add("SuccessfulResponses", SimpleType.INTEGER, connection.getSuccessfulResponses())
					.add("ErrorResponses", SimpleType.INTEGER, connection.getErrorResponses())
					.add("TimeExecution", SimpleType.STRING, connection.getTimeExecutionMillis())
					.add("LastResponseException", lastResponseException)
					.add("LastInternalException", lastInternalException)
					.add("ConnectionDetails", connectionDetails)
					.build());
		}
		return compositeData.toArray(new CompositeData[compositeData.size()]);
	}

	@Override
	public long getTotalRequests() {
		long requests = 0;
		for (RpcServerConnection connection : connections.values()) {
			requests += connection.getSuccessfulResponses() + connection.getErrorResponses();
		}
		return requests;
	}

	@Override
	public long getTotalProcessingErrors() {
		long requests = 0;
		for (RpcServerConnection connection : connections.values()) {
			requests += connection.getErrorResponses();
		}
		return requests;
	}
}

