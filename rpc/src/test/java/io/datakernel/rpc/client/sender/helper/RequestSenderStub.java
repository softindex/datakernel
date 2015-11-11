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

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.sender.RpcRequestSender;

public class RequestSenderStub implements RpcRequestSender {

	private final int id;
	private int sendRequestCalls;

	public RequestSenderStub(int id) {
		this.id = id;
		this.sendRequestCalls = 0;
	}

	@Override
	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		++sendRequestCalls;
	}

	public int getId() {
		return id;
	}

	public int getSendRequestCalls() {
		return sendRequestCalls;
	}

	@Override
	public boolean equals(Object obj) {
		return ((RequestSenderStub) obj).getId() == id;
	}

	@Override
	public int hashCode() {
		return id;
	}
}
