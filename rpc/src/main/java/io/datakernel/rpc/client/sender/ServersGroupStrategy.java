package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public final class ServersGroupStrategy extends AbstractRequestSendingStrategy {

	private List<InetSocketAddress> addresses;

	ServersGroupStrategy(List<InetSocketAddress> addresses) {
		this.addresses = addresses;
	}

	@Override
	protected List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		List<Optional<RequestSender>> senders = new ArrayList<>();
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = pool.get(address);
			if (connection != null) {
				senders.add(
						Optional.<RequestSender>of(new SingleServerStrategy.RequestSenderToSingleServer(connection))
				);
			} else {
				senders.add(Optional.<RequestSender>absent());
			}
		}
		return senders;
	}

	@Override
	public Optional<RequestSender> create(RpcClientConnectionPool pool) {
		throw new UnsupportedOperationException();
	}
}
