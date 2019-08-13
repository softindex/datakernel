package io.datakernel.memcache.client;

import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;
import io.datakernel.rpc.client.IRpcClient;

public class RawMemcacheClient extends AbstractMemcacheClient<byte[], Slice> {
	private RawMemcacheClient(IRpcClient rpcClient) {
		super(rpcClient);
	}

	public static RawMemcacheClient create(IRpcClient rpcClient) {
		return new RawMemcacheClient(rpcClient);
	}

	@Override
	protected byte[] encodeKey(byte[] key) {
		return key;
	}

	@Override
	protected Slice encodeValue(Slice value) {
		return value;
	}

	@Override
	protected Slice decodeValue(Slice slice) {
		return slice;
	}
}
