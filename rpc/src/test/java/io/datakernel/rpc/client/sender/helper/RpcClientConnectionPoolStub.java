package io.datakernel.rpc.client.sender.helper;

import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class RpcClientConnectionPoolStub implements RpcClientConnectionPool {
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();

	public void put(InetSocketAddress address, RpcClientConnection connection) {
		connections.put(address, connection);
	}

	public void remove(InetSocketAddress address) {
		connections.remove(address);
	}

	@Override
	public RpcClientConnection get(InetSocketAddress address) {
		return connections.get(address);
	}
}
