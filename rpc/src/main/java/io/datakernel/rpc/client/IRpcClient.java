package io.datakernel.rpc.client;

import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.rpc.protocol.RpcOverloadException;

import java.util.concurrent.CompletionStage;

public interface IRpcClient {
	AsyncTimeoutException RPC_TIMEOUT_EXCEPTION = new AsyncTimeoutException();
	RpcOverloadException RPC_OVERLOAD_EXCEPTION = new RpcOverloadException();

	<I, O> CompletionStage<O> sendRequest(I request, int timeout);
}
