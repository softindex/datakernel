package io.datakernel.rpc.server;

import io.datakernel.async.ResultCallback;

public interface RpcRequestHandler<R> {
	void run(R request, ResultCallback<Object> callback);
}
