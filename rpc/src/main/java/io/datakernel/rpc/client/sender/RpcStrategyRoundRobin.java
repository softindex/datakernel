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

import java.util.List;

import static io.datakernel.rpc.client.sender.RpcSendersUtils.containsNullValues;
import static io.datakernel.util.Preconditions.checkArgument;

public final class RpcStrategyRoundRobin extends RpcRequestSendingStrategyToGroup implements RpcSingleSenderStrategy {

	public RpcStrategyRoundRobin(List<RpcRequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public RpcStrategyRoundRobin withMinActiveSubStrategies(int minActiveSubStrategies) {
		setMinSubStrategiesForCreation(minActiveSubStrategies);
		return this;
	}

	@Override
	protected RpcRequestSender createSenderInstance(List<RpcRequestSender> subSenders) {
		if (subSenders.size() > 1) {
			return new RequestSenderRoundRobin(subSenders);
		} else {
			assert subSenders.size() == 1;
			return subSenders.get(0);
		}
	}

	final static class RequestSenderRoundRobin implements RpcRequestSender {
		private int nextSender;
		private RpcRequestSender[] subSenders;

		public RequestSenderRoundRobin(List<RpcRequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0 && !containsNullValues(senders));
			this.subSenders = senders.toArray(new RpcRequestSender[senders.size()]);
			this.nextSender = 0;
		}

		@Override
		public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
			RpcRequestSender sender = getCurrentSubSender();
			sender.sendRequest(request, timeout, callback);
		}

		private RpcRequestSender getCurrentSubSender() {
			RpcRequestSender currentSender = subSenders[nextSender];
			nextSender = (nextSender + 1) % subSenders.length;
			return currentSender;
		}
	}
}
