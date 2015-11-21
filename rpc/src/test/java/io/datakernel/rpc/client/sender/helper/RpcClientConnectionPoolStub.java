package io.datakernel.rpc.client.sender.helper;

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.RpcRequestSender;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class RpcClientConnectionPoolStub implements RpcClientConnectionPool {
	private final Map<InetSocketAddress, RpcRequestSender> connections = new HashMap<>();

	public void put(InetSocketAddress address, RpcRequestSender connection) {
		connections.put(address, connection);
	}

	public void remove(InetSocketAddress address) {
		connections.remove(address);
	}

	@Override
	public RpcRequestSender get(InetSocketAddress address) {
		return connections.get(address);
	}
}
