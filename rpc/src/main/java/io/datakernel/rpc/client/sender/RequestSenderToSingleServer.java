package io.datakernel.rpc.client.sender;


import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;
import java.net.InetSocketAddress;

import static com.google.common.base.Preconditions.checkNotNull;


final class RequestSenderToSingleServer implements RequestSender {
	private final RpcClientConnection connection;
	private final boolean active;

	public RequestSenderToSingleServer(InetSocketAddress address, RpcClientConnectionPool connectionPool) {
		checkNotNull(address);
		checkNotNull(connectionPool);
		this.connection = connectionPool.get(address);
		this.active = this.connection != null;
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
	                                                              ResultCallback<T> callback) {
		assert active;

		connection.callMethod(request, timeout, callback);
	}

	@Override
	public boolean isActive() {
		return active;
	}
}
