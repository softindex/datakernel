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

import com.google.common.base.Predicate;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.rpc.client.sender.RpcSendersUtils.containsNullValues;
import static io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

public final class RpcStrategyFirstValidResult extends RpcRequestSendingStrategyToGroup implements RpcSingleSenderStrategy {
	private static final Predicate<? extends RpcMessageData> DEFAULT_RESULT_VALIDATOR = new DefaultResultValidator<>();

	private Predicate<? extends RpcMessageData> resultValidator;
	private Exception noValidResultException;

	public RpcStrategyFirstValidResult(List<RpcRequestSendingStrategy> subStrategies) {
		super(subStrategies);
		resultValidator = DEFAULT_RESULT_VALIDATOR;
		noValidResultException = null;
	}

	public RpcStrategyFirstValidResult withMinActiveSubStrategies(int minActiveSubStrategies) {
		setMinSubStrategiesForCreation(minActiveSubStrategies);
		return this;
	}

	public RpcStrategyFirstValidResult withResultValidator(Predicate<? extends RpcMessageData> resultValidator) {
		this.resultValidator = resultValidator;
		return this;
	}

	public <T extends RpcMessageData> RpcStrategyFirstValidResult withNoValidResultException(Exception exception) {
		this.noValidResultException = exception;
		return this;
	}

	@Override
	protected RpcRequestSender createSenderInstance(List<RpcRequestSender> subSenders) {
		return new RequestSenderToAll(subSenders, resultValidator, noValidResultException);
	}

	final static class RequestSenderToAll implements RpcRequestSender {

		private final RpcRequestSender[] subSenders;
		private final Predicate<? extends RpcMessageData> resultValidator;
		private final Exception noValidResultException;

		public RequestSenderToAll(List<RpcRequestSender> senders, Predicate<? extends RpcMessageData> resultValidator,
		                          Exception noValidResultException) {
			checkArgument(senders != null && senders.size() > 0 && !containsNullValues(senders));
			;
			this.subSenders = senders.toArray(new RpcRequestSender[senders.size()]);
			this.resultValidator = checkNotNull(resultValidator);
			this.noValidResultException = noValidResultException;
		}

		@Override
		public <T extends RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                   final ResultCallback<T> callback) {
			// TODO (vmykhalko): is there all right with generics ?
			FirstResultCallback<T> resultCallback
					= new FirstResultCallback<>(callback, (Predicate<T>) resultValidator, subSenders.length, noValidResultException);
			for (RpcRequestSender sender : subSenders) {
				sender.sendRequest(request, timeout, resultCallback);
			}
		}
	}

	private static final class FirstResultCallback<T> implements ResultCallback<T> {
		private final ResultCallback<T> resultCallback;
		private final Predicate<T> resultValidator;
		private final Exception noValidResultException;
		private int expectedCalls;
		private T result;
		private Exception exception;
		private boolean hasResult;
		private boolean complete;

		public FirstResultCallback(ResultCallback<T> resultCallback, Predicate<T> resultValidator, int expectedCalls,
		                           Exception noValidResultException) {
			checkArgument(expectedCalls > 0);
			this.expectedCalls = expectedCalls;
			this.resultCallback = checkNotNull(resultCallback);
			this.resultValidator = checkNotNull(resultValidator);
			this.noValidResultException = noValidResultException;
		}

		@Override
		public final void onResult(T result) {
			--expectedCalls;
			if (!hasResult && resultValidator.apply(result)) {
				this.result = result;  // first valid result
				this.hasResult = true;
			}
			processResult();
		}

		@Override
		public final void onException(Exception exception) {
			--expectedCalls;
			if (!hasResult) {
				this.exception = exception; // last Exception
			}
			processResult();
		}

		private boolean resultReady() {
			return hasResult || expectedCalls == 0;
		}

		private void processResult() {
			if (complete || !resultReady()) {
				return;
			}
			complete = true;
			if (hasResult) {
				resultCallback.onResult(result);
			} else {
				if (exception == null) {   // TODO (vmykhalko): try to assign noValidResultException to exception, maybe it will simplify this code
					if (noValidResultException != null) {
						resultCallback.onException(noValidResultException);
					} else {
						resultCallback.onResult(null);
					}
				} else {
					resultCallback.onException(exception);
				}
			}
		}
	}

	private static final class DefaultResultValidator<T> implements Predicate<T> {
		@Override
		public boolean apply(T input) {
			return input != null;
		}
	}
}
