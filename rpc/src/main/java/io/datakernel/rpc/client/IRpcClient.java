package io.datakernel.rpc.client;

import io.datakernel.async.ResultCallback;

public interface IRpcClient {
	<I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback);
}
