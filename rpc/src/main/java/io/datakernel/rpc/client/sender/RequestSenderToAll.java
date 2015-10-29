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

package io.datakernel.rpc.client.sender;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import io.datakernel.async.FirstResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.protocol.RpcMessage;


final class RequestSenderToAll extends RequestSenderToGroup {

	public RequestSenderToAll(List<RequestSender> senders) {
		super(senders);
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
	                                                              final ResultCallback<T> callback) {
		checkNotNull(callback);
		List<RequestSender> activeSubSenders = getActiveSubSenders();

		assert isActive();

		FirstResultCallback<T> resultCallback = new FirstResultCallback<>(callback);
		for (RequestSender sender : activeSubSenders) {
			sender.sendRequest(request, timeout, callback);
		}
		resultCallback.resultOf(activeSubSenders.size());
	}
}
