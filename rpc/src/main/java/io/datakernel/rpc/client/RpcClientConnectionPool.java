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

import io.datakernel.jmx.CompositeDataBuilder;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.net.InetSocketAddress;
import java.util.*;

public final class RpcClientConnectionPool implements RpcClientConnectionPoolMBean {
	private final List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> pool;

	// JMX
	private boolean monitoring;

	public RpcClientConnectionPool(List<InetSocketAddress> addresses) {
		this.addresses = Collections.unmodifiableList(new ArrayList<>(addresses));
		this.pool = new HashMap<>(addresses.size());
	}

	public RpcClientConnection get(InetSocketAddress key) {
		return pool.get(key);
	}

	public List<InetSocketAddress> addresses() {
		return addresses;
	}

	public Collection<InetSocketAddress> activeAddresses() {
		return pool.keySet();
	}

	public void add(InetSocketAddress address, RpcClientConnection connection) {
		assert addresses.contains(address);

		pool.put(address, connection);
		if (monitoring)
			connection.startMonitoring();
	}

	public void remove(InetSocketAddress address) {
		pool.remove(address);
	}

	public Collection<RpcClientConnection> values() {
		return new ArrayList<>(pool.values());
	}

	public int size() {
		return pool.size();
	}

	// JMX
	@Override
	public void startMonitoring() {
		monitoring = true;
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
			if (connection != null)
				connection.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
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
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
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
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
			if (connection == null)
				continue;

			CompositeData lastTimeoutException = connection.getLastTimeoutException();
			CompositeData lastRemoteException = connection.getLastRemoteException();
			CompositeData lastProtocolException = connection.getLastProtocolException();
			CompositeData connectionDetails = connection.getConnectionDetails();

			compositeData.add(CompositeDataBuilder.builder("Rpc connections", "Rpc connections status")
					.add("Address", SimpleType.STRING, address.toString())
					.add("SuccessfulRequests", SimpleType.INTEGER, connection.getSuccessfulRequests())
					.add("FailedRequests", SimpleType.INTEGER, connection.getFailedRequests())
					.add("RejectedRequests", SimpleType.INTEGER, connection.getRejectedRequests())
					.add("ExpiredRequests", SimpleType.INTEGER, connection.getExpiredRequests())
					.add("PendingRequests", SimpleType.STRING, connection.getPendingRequestsStats())
					.add("ProcessResultTimeMicros", SimpleType.STRING, connection.getProcessResultTimeStats())
					.add("ProcessExceptionTimeMicros", SimpleType.STRING, connection.getProcessExceptionTimeStats())
					.add("SendPacketTimeMicros", SimpleType.STRING, connection.getSendPacketTimeStats())
					.add("LastTimeoutException", lastTimeoutException)
					.add("LastRemoteException", lastRemoteException)
					.add("LastProtocolException", lastProtocolException)
					.add("ConnectionDetails", connectionDetails)
					.build());
		}
		return compositeData.toArray(new CompositeData[compositeData.size()]);
	}

	@Override
	public long getTotalSuccessfulRequests() {
		if (pool.isEmpty())
			return 0;
		long result = 0;
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
			if (connection == null)
				continue;
			result += connection.getSuccessfulRequests();
		}
		return result;
	}

	@Override
	public long getTotalPendingRequests() {
		if (pool.isEmpty())
			return 0;
		long result = 0;
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
			if (connection == null)
				continue;
			result += connection.getPendingRequests();
		}
		return result;
	}

	@Override
	public long getTotalRejectedRequests() {
		if (pool.isEmpty())
			return 0;
		long result = 0;
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
			if (connection == null)
				continue;
			result += connection.getRejectedRequests();
		}
		return result;
	}

	@Override
	public long getTotalFailedRequests() {
		if (pool.isEmpty())
			return 0;
		long result = 0;
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
			if (connection == null)
				continue;
			result += connection.getFailedRequests();
		}
		return result;
	}

	@Override
	public long getTotalExpiredRequests() {
		if (pool.isEmpty())
			return 0;
		long result = 0;
		for (InetSocketAddress address : new HashSet<>(pool.keySet())) {
			RpcClientConnection connection = pool.get(address);
			if (connection == null)
				continue;
			result += connection.getExpiredRequests();
		}
		return result;
	}
}
