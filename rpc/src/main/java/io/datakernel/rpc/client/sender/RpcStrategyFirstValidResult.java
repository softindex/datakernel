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
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategyFirstValidResult implements RpcStrategy {
	public interface ResultValidator<T> {
		boolean isValidResult(T value);
	}

	private static final ResultValidator<?> DEFAULT_RESULT_VALIDATOR = new DefaultResultValidator<>();

	private final RpcStrategyList list;

	private ResultValidator<?> resultValidator;
	private Exception noValidResultException;

	public RpcStrategyFirstValidResult(RpcStrategyList list) {
		this.list = list;
		this.resultValidator = DEFAULT_RESULT_VALIDATOR;
		this.noValidResultException = null;
	}

	public RpcStrategyFirstValidResult withResultValidator(ResultValidator<?> resultValidator) {
		this.resultValidator = resultValidator;
		return this;
	}

	public RpcStrategyFirstValidResult withNoValidResultException(Exception exception) {
		this.noValidResultException = exception;
		return this;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return list.getAddresses();
	}

	@Override
	public RpcSender createSender(RpcClientConnectionPool pool) {
		List<RpcSender> senders = list.listOfSenders(pool);
		if (senders.size() == 0)
			return null;
		return new Sender(senders, resultValidator, noValidResultException);
	}

	static final class Sender implements RpcSender {
		private final RpcSender[] subSenders;
		private final ResultValidator<?> resultValidator;
		private final Exception noValidResultException;

		public Sender(List<RpcSender> senders, ResultValidator<?> resultValidator,
		              Exception noValidResultException) {
			checkArgument(senders != null && senders.size() > 0);
			this.subSenders = senders.toArray(new RpcSender[senders.size()]);
			this.resultValidator = checkNotNull(resultValidator);
			this.noValidResultException = noValidResultException;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			FirstResultCallback<O> resultCallback
					= new FirstResultCallback<>(callback, (ResultValidator<O>) resultValidator, subSenders.length, noValidResultException);
			for (RpcSender sender : subSenders) {
				sender.sendRequest(request, timeout, resultCallback.getCallback());
			}
		}
	}

	private static final class FirstResultCallback<T> {
		private final ResultCallback<T> resultCallback;
		private final ResultValidator<T> resultValidator;
		private final Exception noValidResultException;
		private int expectedCalls;
		private T result;
		private Exception exception;
		private boolean hasResult;
		private boolean complete;

		public FirstResultCallback(ResultCallback<T> resultCallback, ResultValidator<T> resultValidator, int expectedCalls,
		                           Exception noValidResultException) {
			checkArgument(expectedCalls > 0);
			this.expectedCalls = expectedCalls;
			this.resultCallback = checkNotNull(resultCallback);
			this.resultValidator = checkNotNull(resultValidator);
			this.noValidResultException = noValidResultException;
		}

		public ResultCallback<T> getCallback() {
			return new ResultCallback<T>() {
				@Override
				protected void onResult(T result) {
					--expectedCalls;
					if (!hasResult && resultValidator.isValidResult(result)) {
						FirstResultCallback.this.result = result;  // first valid result
						FirstResultCallback.this.hasResult = true;
					}
					processResult();
				}

				@Override
				protected void onException(Exception exception) {
					--expectedCalls;
					if (!hasResult) {
						FirstResultCallback.this.exception = exception; // last Exception
					}
					processResult();
				}
			};
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
				resultCallback.sendResult(result);
			} else {
				resolveException();
				if (exception == null) {
					resultCallback.sendResult(null);
				} else {
					resultCallback.fireException(exception);
				}
			}
		}

		private void resolveException() {
			if (exception == null && noValidResultException != null) {
				exception = noValidResultException;
			}
		}
	}

	private static final class DefaultResultValidator<T> implements ResultValidator<T> {
		@Override
		public boolean isValidResult(T input) {
			return input != null;
		}
	}

}
