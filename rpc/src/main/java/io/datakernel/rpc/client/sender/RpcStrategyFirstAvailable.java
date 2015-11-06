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
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class RpcStrategyFirstAvailable extends RpcRequestSendingStrategyToGroup implements RpcSingleSenderStrategy {

	public RpcStrategyFirstAvailable(List<RpcRequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public RpcStrategyFirstAvailable(List<RpcRequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RpcRequestSender createSenderInstance(List<RpcRequestSender> subSenders) {
		return new RequestSenderToFirst(subSenders);
	}

	final static class RequestSenderToFirst implements RpcRequestSender {
		private final RpcRequestSender first;

		public RequestSenderToFirst(List<RpcRequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0);
			this.first = checkNotNull(senders.get(0));
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {
			checkNotNull(callback);

			first.sendRequest(request, timeout, callback);
		}
	}
}
