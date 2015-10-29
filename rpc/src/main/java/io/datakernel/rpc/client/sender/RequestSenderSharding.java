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

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderSharding extends RequestSenderToGroup {
	private static final RpcNoSenderAvailableException NO_ACTIVE_SENDER_AVAILABLE_EXCEPTION
			= new RpcNoSenderAvailableException("No active senders available");
	private final HashFunction<RpcMessage.RpcMessageData> hashFunction;

	public RequestSenderSharding(List<RequestSender> senders, HashFunction<RpcMessage.RpcMessageData> hashFunction) {
		super(senders);
		this.hashFunction = checkNotNull(hashFunction);
	}


	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
	                                                              final ResultCallback<T> callback) {
		checkNotNull(callback);

		assert isActive();

		RequestSender sender = chooseSender(request);
		if (sender.isActive()) {
			sender.sendRequest(request, timeout, callback);
			return;
		}
		callback.onException(NO_ACTIVE_SENDER_AVAILABLE_EXCEPTION);
	}

	private RequestSender chooseSender(RpcMessage.RpcMessageData request) {
		List<RequestSender> subSenders = getAllSubSenders();
		int index = Math.abs(hashFunction.hashCode(request)) % subSenders.size();
		return subSenders.get(index);
	}
}
