package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import java.net.InetSocketAddress;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SingleServerStrategy extends AbstractRequestSendingStrategy {

	private final InetSocketAddress address;

	public SingleServerStrategy(InetSocketAddress address) {
		this.address = address;
	}

	@Override
	protected List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		return null;
	}

	@Override
	public Optional<RequestSender> create(RpcClientConnectionPool pool) {
		RpcClientConnection connection = pool.get(address);
		if (connection != null) {
			return Optional.<RequestSender>of(new RequestSenderToSingleServer(connection));
		} else {
			return Optional.absent();
		}
	}

	final static class RequestSenderToSingleServer implements RequestSender {
		private final RpcClientConnection connection;

		public RequestSenderToSingleServer(RpcClientConnection connection) {
			this.connection = checkNotNull(connection);
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {

			connection.callMethod(request, timeout, callback);
		}
	}
}
