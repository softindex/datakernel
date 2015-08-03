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
import io.datakernel.rpc.protocol.RpcMessageSerializer;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.server.RpcServerConnection.StatusListener;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.nio.channels.SocketChannel;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RpcServer extends AbstractNioServer<RpcServer> implements RpcServerMBean {

	public static class Builder {
		private final NioEventloop eventloop;
		private RequestHandlers handlers;
		private RpcMessageSerializer serializer;
		private RpcProtocolFactory protocolFactory;

		public Builder(NioEventloop eventloop) {
			this.eventloop = checkNotNull(eventloop);
		}

		public Builder requestHandlers(RequestHandlers handlers) {
			this.handlers = handlers;
			return this;
		}

		public Builder serializer(RpcMessageSerializer serializer) {
			this.serializer = serializer;
			return this;
		}

		public Builder protocolFactory(RpcProtocolFactory protocolFactory) {
			this.protocolFactory = protocolFactory;
			return this;
		}

		public RpcServer build() {
			checkNotNull(serializer, "RpcMessageSerializer is no set");
			checkNotNull(protocolFactory, "RpcProtocolFactory is no set");
			return new RpcServer(this);
		}
	}

	private final RpcServerConnectionPool connections = new RpcServerConnectionPool();
	private final RequestHandlers handlers;
	private final RpcProtocolFactory protocolFactory;
	private final RpcMessageSerializer serializer;

	private RpcServer(Builder builder) {
		super(builder.eventloop);
		this.handlers = checkNotNull(builder.handlers, "RequestHandlers is not set");
		this.protocolFactory = builder.protocolFactory;
		this.serializer = builder.serializer;
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
		RpcServerConnection serverConnection = new RpcServerConnection(eventloop, socketChannel, serializer, handlers, protocolFactory, statusListener);
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

