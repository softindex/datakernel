package io.datakernel.rpc.client.sender;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import java.net.InetAddress;
import java.net.InetSocketAddress;

final class RequestSenderToSingleServer implements RequestSender {
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();
	private static final HashFunction DEFAULT_HASH_FUNCTION = Hashing.murmur3_32();

	private final RpcClientConnectionPool connectionPool;
	private final InetSocketAddress address;
	private final HashFunction hashFunction;
	private final int key;
	private final boolean active;

	public RequestSenderToSingleServer(InetSocketAddress address, RpcClientConnectionPool connectionPool,
										HashFunction hashFunction) {
		this.connectionPool = connectionPool;
		this.address = address;
		this.hashFunction = hashFunction;
		this.key = computeKey();
		this.active = checkConnectionAvailable();
	}

	public RequestSenderToSingleServer(InetSocketAddress address, RpcClientConnectionPool connectionPool) {
		this(address, connectionPool, DEFAULT_HASH_FUNCTION);
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

	private int computeKey() {
		return hashFunction.newHasher()
				.putInt(ipv4ToInt(address.getAddress()))
				.putInt(address.getPort())
				.hash().asInt();
	}

	private static int ipv4ToInt(InetAddress address) {
		byte[] ipAddressBytes = address.getAddress();
		int result = ipAddressBytes[0] & 0xff;
		result |= (ipAddressBytes[1] << 8) & 0xff00;
		result |= (ipAddressBytes[2] << 16) & 0xff0000;
		result |= (ipAddressBytes[3] << 24) & 0xff000000;
		return result;
	}
}
