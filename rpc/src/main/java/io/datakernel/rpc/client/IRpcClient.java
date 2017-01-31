package io.datakernel.rpc.client;

import io.datakernel.async.ResultCallback;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.rpc.protocol.RpcOverloadException;

public interface IRpcClient {
	AsyncTimeoutException RPC_TIMEOUT_EXCEPTION = new AsyncTimeoutException();
	RpcOverloadException RPC_OVERLOAD_EXCEPTION = new RpcOverloadException();

	<I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback);
}
