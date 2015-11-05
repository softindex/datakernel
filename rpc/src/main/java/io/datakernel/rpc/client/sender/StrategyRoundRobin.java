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
import static io.datakernel.rpc.client.sender.RpcSendersUtils.containsNullValues;

public final class StrategyRoundRobin extends RequestSendingStrategyToGroup implements SingleSenderStrategy {

	public StrategyRoundRobin(List<RequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public StrategyRoundRobin(List<RequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RequestSender createSenderInstance(List<RequestSender> subSenders) {
		return new RequestSenderRoundRobin(subSenders);
	}

	final static class RequestSenderRoundRobin implements RequestSender {
		private int nextSender;
		private RequestSender[] subSenders;

		public RequestSenderRoundRobin(List<RequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0 && !containsNullValues(senders));
			this.subSenders = senders.toArray(new RequestSender[senders.size()]);
			this.nextSender = 0;
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender sender = getCurrentSubSender();
			sender.sendRequest(request, timeout, callback);
		}

		private RequestSender getCurrentSubSender() {
			RequestSender currentSender = subSenders[nextSender];
			nextSender = (nextSender + 1) % subSenders.length;
			return currentSender;
		}
	}
}
