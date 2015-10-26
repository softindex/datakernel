package io.datakernel.rpc.client.sender;


import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;
import java.net.InetSocketAddress;

import static io.datakernel.rpc.client.sender.RequestSenderUtils.EMPTY_KEY;

final class RequestSenderToSingleServer implements RequestSender {
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();

	private final RpcClientConnectionPool connectionPool;
	private final InetSocketAddress address;
	private final int key;
	private final boolean active;

	public RequestSenderToSingleServer(InetSocketAddress address, RpcClientConnectionPool connectionPool,
										int key) {
		this.connectionPool = connectionPool;
		this.address = address;
		this.key = key;
		this.active = checkConnectionAvailable();
	}

//	public RequestSenderToSingleServer(InetSocketAddress address, RpcClientConnectionPool connectionPool) {
//		this(address, connectionPool, EMPTY_KEY);
//	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
	                                                              ResultCallback<T> callback) {
		// TODO (vmykhalko): maybe try to cache connection during creation of strategy ?
		RpcClientConnection connection = connectionPool.get(address);
		if (connection != null) {
			connection.callMethod(request, timeout, callback);
		} else {
			callback.onException(NO_AVAILABLE_CONNECTION);
		}
	}

	@Override
	public int getKey() {
		return key;
	}

	@Override
	public boolean isActive() {
		return active;
	}

	private boolean checkConnectionAvailable() {
		return connectionPool.get(address) != null;
	}
}
