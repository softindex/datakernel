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
import io.datakernel.util.Preconditions;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.rpc.client.sender.RpcSendersUtils.containsNullValues;
import static io.datakernel.util.Preconditions.check;

public final class RpcStrategyAllAvailable extends RpcRequestSendingStrategyToGroup implements RpcSingleSenderStrategy {

	public RpcStrategyAllAvailable(List<RpcRequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public RpcStrategyAllAvailable(List<RpcRequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RpcRequestSender createSenderInstance(List<RpcRequestSender> subSenders) {
		return new RequestSenderToAll(subSenders);
	}

	final static class RequestSenderToAll implements RpcRequestSender {

		private final RpcRequestSender[] subSenders;

		public RequestSenderToAll(List<RpcRequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0 && !containsNullValues(senders));
			this.subSenders = senders.toArray(new RpcRequestSender[senders.size()]);
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              final ResultCallback<T> callback) {
			checkNotNull(callback);

			FirstResultCallback<T> resultCallback = new FirstResultCallback<>(callback);
			for (RpcRequestSender sender : subSenders) {
				sender.sendRequest(request, timeout, resultCallback);
			}
			resultCallback.resultOf(subSenders.length);
		}
	}

	private static final class FirstResultCallback<T> implements ResultCallback<T> {
		private final ResultCallback<T> resultCallback;
		private T result;
		private Exception exception;
		private int countCalls;
		private int awaitsCalls;
		private boolean hasResult;
		private boolean complete;

		public FirstResultCallback(ResultCallback<T> resultCallback) {
			Preconditions.checkNotNull(resultCallback);
			this.resultCallback = resultCallback;
		}

		@Override
		public final void onResult(T result) {
			++countCalls;
			if (!hasResult && isValidResult(result)) {
				this.result = result;  // first valid result
				this.hasResult = true;
			}
			processResult();
		}

		protected boolean isValidResult(T result) {
			return result != null;
		}

		@Override
		public final void onException(Exception exception) {
			++countCalls;
			if (!hasResult) {
				this.exception = exception; // last Exception
			}
			processResult();
		}

		public void resultOf(int maxAwaitsCalls) {
			check(maxAwaitsCalls > 0);
			this.awaitsCalls = maxAwaitsCalls;
			processResult();
		}

		private boolean resultReady() {
			return awaitsCalls > 0 && (countCalls == awaitsCalls || hasResult);
		}

		private void processResult() {
			if (complete || !resultReady()) return;
			complete = true;
			if (hasResult || exception == null)
				resultCallback.onResult(result);
			else
				resultCallback.onException(exception);
		}
	}
}
