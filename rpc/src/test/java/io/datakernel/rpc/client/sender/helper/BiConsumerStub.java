package io.datakernel.rpc.client.sender.helper;

import java.util.function.BiConsumer;

public class BiConsumerStub implements BiConsumer<RpcMessageDataStub, Throwable> {

	@Override
	public void accept(RpcMessageDataStub rpcMessageDataStub, Throwable throwable) {
		throw new UnsupportedOperationException();
	}
}
