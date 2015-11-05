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

public final class StrategyFirstAvailable extends RequestSendingStrategyToGroup implements SingleSenderStrategy {

	public StrategyFirstAvailable(List<RequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public StrategyFirstAvailable(List<RequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RequestSender createSenderInstance(List<RequestSender> subSenders) {
		return new RequestSenderToFirst(subSenders);
	}

	final static class RequestSenderToFirst implements RequestSender {
		private final RequestSender first;

		public RequestSenderToFirst(List<RequestSender> senders) {
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
