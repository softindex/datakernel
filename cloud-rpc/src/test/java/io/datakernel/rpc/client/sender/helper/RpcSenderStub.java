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

package io.datakernel.rpc.client.sender.helper;

import io.datakernel.async.callback.Callback;
import io.datakernel.rpc.client.sender.RpcSender;
import org.jetbrains.annotations.NotNull;

public final class RpcSenderStub implements RpcSender {
	private int requests;

	public int getRequests() {
		return requests;
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
		requests++;
	}
}
