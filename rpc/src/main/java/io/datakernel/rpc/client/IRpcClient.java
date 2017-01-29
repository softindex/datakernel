package io.datakernel.rpc.client;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.protocol.RpcOverloadException;
import io.datakernel.rpc.protocol.RpcTimeoutException;

public interface IRpcClient {
	RpcTimeoutException RPC_TIMEOUT_EXCEPTION = new RpcTimeoutException();
	RpcOverloadException RPC_OVERLOAD_EXCEPTION = new RpcOverloadException();

	<I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback);
}
