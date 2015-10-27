package io.datakernel.rpc.client.sender;


import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;
import java.net.InetSocketAddress;


final class RequestSenderToSingleServer implements RequestSender {
	// TODO (vmykhalko): if do not use caching of connection, another exception class is probably needed
	private static final RpcNoSenderAvailableException NO_AVAILABLE_CONNECTION
			= new RpcNoSenderAvailableException("No available connection");

	private final RpcClientConnectionPool connectionPool;
	private final InetSocketAddress address;
	private final boolean active;

	public RequestSenderToSingleServer(InetSocketAddress address, RpcClientConnectionPool connectionPool) {
		this.connectionPool = connectionPool;
		this.address = address;
		this.active = checkConnectionAvailable();
	}

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
	public boolean isActive() {
		return active;
	}

	private boolean checkConnectionAvailable() {
		return connectionPool.get(address) != null;
	}
}
