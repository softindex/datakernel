package io.datakernel.rpc.client.sender.helper;

import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;

public class RpcMessageDataStubWithKeyHashFunction implements HashFunction<RpcMessage.RpcMessageData> {

	@Override
	public int hashCode(RpcMessage.RpcMessageData item) {
		return ((RpcMessageDataStubWithKey) item).getKey();
	}
}
