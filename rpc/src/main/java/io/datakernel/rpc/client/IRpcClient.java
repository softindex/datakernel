/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.rpc.protocol.RpcOverloadException;

public interface IRpcClient {
	AsyncTimeoutException RPC_TIMEOUT_EXCEPTION = new AsyncTimeoutException("RPC");
	RpcOverloadException RPC_OVERLOAD_EXCEPTION = new RpcOverloadException();

	default <I, O> Stage<O> sendRequest(I request, int timeout) {
		SettableStage<O> result = SettableStage.create();
		sendRequest(request, timeout, result);
		return result;
	}

	<I, O> void sendRequest(I request, int timeout, Callback<O> cb);
}
