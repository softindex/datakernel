/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.rpc.client;

import io.datakernel.async.callback.Callback;
import io.datakernel.common.exception.AsyncTimeoutException;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import io.datakernel.rpc.protocol.RpcOverloadException;

public interface IRpcClient {
	AsyncTimeoutException RPC_TIMEOUT_EXCEPTION = new AsyncTimeoutException(IRpcClient.class, "RPC request has timed out");
	RpcOverloadException RPC_OVERLOAD_EXCEPTION = new RpcOverloadException(IRpcClient.class, "RPC client is overloaded");

	default <I, O> Promise<O> sendRequest(I request, int timeout) {
		SettablePromise<O> resultPromise = new SettablePromise<>();
		sendRequest(request, timeout, resultPromise);
		return resultPromise;
	}

	default <I, O> Promise<O> sendRequest(I request) {
		SettablePromise<O> resultPromise = new SettablePromise<>();
		sendRequest(request, resultPromise);
		return resultPromise;
	}

	<I, O> void sendRequest(I request, int timeout, Callback<O> cb);

	default <I, O> void sendRequest(I request, Callback<O> cb) {
		sendRequest(request, Integer.MAX_VALUE, cb);
	}
}
