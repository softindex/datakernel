package io.datakernel.rpc.client.sender.helper;

import io.datakernel.async.ResultCallback;

public class ResultCallbackStub implements ResultCallback<RpcMessageDataStub> {
	@Override
	public void onResult(RpcMessageDataStub result) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void onException(Exception exception) {
		throw new UnsupportedOperationException();
	}
}
