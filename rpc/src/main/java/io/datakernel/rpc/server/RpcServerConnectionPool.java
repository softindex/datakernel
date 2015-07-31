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

import io.datakernel.jmx.CompositeDataBuilder;
import org.slf4j.Logger;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.nio.channels.SocketChannel;
import java.util.*;

import static org.slf4j.LoggerFactory.getLogger;

public final class RpcServerConnectionPool implements RpcServerConnectionPoolMBean {
	private static final Logger logger = getLogger(RpcServerConnectionPool.class);

	private final Map<SocketChannel, RpcServerConnection> pool = new HashMap<>();

	// JMX
	private boolean monitoring;

	public void add(SocketChannel socketChannel, RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client connected on {}", socketChannel);
		pool.put(socketChannel, connection);
	}

	public void remove(SocketChannel socketChannel) {
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", socketChannel);
		pool.remove(socketChannel);
	}

	public Collection<RpcServerConnection> values() {
		return new ArrayList<>(pool.values());
	}

	public int size() {
		return pool.size();
	}

	// JMX
	@Override
	public void startMonitoring() {
		monitoring = true;
		for (SocketChannel socketChannel : new HashSet<>(pool.keySet())) {
			RpcServerConnection connection = pool.get(socketChannel);
			if (connection != null)
				connection.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (SocketChannel socketChannel : new HashSet<>(pool.keySet())) {
			RpcServerConnection connection = pool.get(socketChannel);
			if (connection != null)
				connection.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		for (SocketChannel socketChannel : new HashSet<>(pool.keySet())) {
			RpcServerConnection connection = pool.get(socketChannel);
			if (connection != null)
				connection.reset();
		}
	}

	@Override
	public int getConnectionsCount() {
		return size();
	}

	@Override
	public CompositeData[] getConnections() throws OpenDataException {
		if (pool.isEmpty())
			return null;
		List<CompositeData> compositeData = new ArrayList<>();
		for (SocketChannel socketChannel : new HashSet<>(pool.keySet())) {
			RpcServerConnection connection = pool.get(socketChannel);
			if (connection == null)
				continue;
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
		if (pool.isEmpty())
			return 0;
		long requests = 0;
		for (SocketChannel socketChannel : new HashSet<>(pool.keySet())) {
			RpcServerConnection connection = pool.get(socketChannel);
			if (connection == null)
				continue;
			requests += connection.getSuccessfulResponses() + connection.getErrorResponses();
		}
		return requests;
	}

	@Override
	public long getTotalProcessingErrors() {
		if (pool.isEmpty())
			return 0;
		long requests = 0;
		for (SocketChannel socketChannel : new HashSet<>(pool.keySet())) {
			RpcServerConnection connection = pool.get(socketChannel);
			if (connection == null)
				continue;
			requests += connection.getErrorResponses();
		}
		return requests;
	}
}
