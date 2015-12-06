package io.datakernel.rpc.server;

import io.datakernel.async.ResultCallback;

public interface RpcRequestHandler<I> {
	void run(I request, ResultCallback<Object> callback);
}
