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
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.protocol.RpcSerializer;
import io.datakernel.rpc.server.RpcServerConnection.StatusListener;
import io.datakernel.serializer.BufferSerializer;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;

public final class RpcServer extends AbstractNioServer<RpcServer> implements RpcServerMBean {
	private final Map<Class<?>, RpcRequestHandler<Object>> handlers = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private final RpcSerializer serializerFactory;

	private final RpcServerConnectionPool connections = new RpcServerConnectionPool();

	private RpcServer(NioEventloop eventloop, RpcSerializer serializerFactory) {
		super(eventloop);
		this.serializerFactory = serializerFactory;
	}

	public static RpcServer create(NioEventloop eventloop, RpcSerializer serializerFactory) {
		return new RpcServer(eventloop, serializerFactory);
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
				connections.add(socketChannel, connection);
			}

			@Override
			public void onClosed() {
				connections.remove(socketChannel);
			}
		};
		BufferSerializer<RpcMessage> messageSerializer = serializerFactory.createSerializer();
		RpcServerConnection serverConnection = new RpcServerConnection(eventloop, socketChannel,
				messageSerializer, messageSerializer,
				handlers, protocolFactory, statusListener);
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
	@Override
	public void startMonitoring() {
		connections.startMonitoring();
	}

	@Override
	public void stopMonitoring() {
		connections.stopMonitoring();
	}

	@Override
	public boolean isMonitoring() {
		return connections.isMonitoring();
	}

	@Override
	public int getConnectionsCount() {
		return connections.size();
	}

	@Override
	public CompositeData[] getConnections() throws OpenDataException {
		return connections.getConnections();
	}

	@Override
	public long getTotalRequests() {
		return connections.getTotalRequests();
	}

	@Override
	public long getTotalProcessingErrors() {
		return connections.getTotalProcessingErrors();
	}
}

