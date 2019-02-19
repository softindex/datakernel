package io.datakernel.memcache.client;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.GetRequest;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.GetResponse;
import io.datakernel.memcache.protocol.MemcacheRpcMessage.PutRequest;
import io.datakernel.rpc.client.IRpcClient;

public final class MemcacheClientImpl implements MemcacheClient {
	private final IRpcClient rpcClient;

	MemcacheClientImpl(IRpcClient rpcClient) {
		this.rpcClient = rpcClient;
	}

	@Override
	public Promise<Void> put(byte[] key, ByteBuf buf, int timeout) {
		PutRequest request = new PutRequest(key, buf);
		return rpcClient.sendRequest(request, timeout).toVoid();
	}

	@Override
	public Promise<ByteBuf> get(byte[] key, int timeout) {
		GetRequest request = new GetRequest(key);
		return rpcClient.<GetRequest, GetResponse>sendRequest(request, timeout)
				.thenApply(response -> {
					if (response == null) {
						return null;
					}
					return response.getData();
				});
	}
}