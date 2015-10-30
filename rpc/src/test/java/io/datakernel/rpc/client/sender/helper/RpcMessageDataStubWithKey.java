package io.datakernel.rpc.client.sender.helper;

import io.datakernel.rpc.protocol.RpcMessage;

public class RpcMessageDataStubWithKey implements RpcMessage.RpcMessageData {

	private final int key;

	public RpcMessageDataStubWithKey(int key) {
		this.key = key;
	}

	public int getKey() {
		return key;
	}

	@Override
	public boolean isMandatory() {
		return false;
	}
}
