package io.datakernel.rpc.client.sender.helper;

import io.datakernel.rpc.protocol.RpcMessage;

public class RpcMessageDataStub implements RpcMessage.RpcMessageData {
	@Override
	public boolean isMandatory() {
		throw new UnsupportedOperationException();
	}
}
