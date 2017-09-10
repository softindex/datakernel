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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategyFirstValidResult implements RpcStrategy {
	public interface ResultValidator<T> {
		boolean isValidResult(T value);
	}

	private static final ResultValidator<?> DEFAULT_RESULT_VALIDATOR = new DefaultResultValidator<>();

	private final RpcStrategyList list;

	private final ResultValidator<?> resultValidator;
	private final Exception noValidResultException;

	private RpcStrategyFirstValidResult(RpcStrategyList list, ResultValidator<?> resultValidator,
	                                    Exception noValidResultException) {
		this.list = list;
		this.resultValidator = resultValidator;
		this.noValidResultException = noValidResultException;
	}

	public static RpcStrategyFirstValidResult create(RpcStrategyList list) {
		return new RpcStrategyFirstValidResult(list, DEFAULT_RESULT_VALIDATOR, null);
	}

	public RpcStrategyFirstValidResult withResultValidator(ResultValidator<?> resultValidator) {
		return new RpcStrategyFirstValidResult(list, resultValidator, noValidResultException);
	}

	public RpcStrategyFirstValidResult withNoValidResultException(Exception exception) {
		return new RpcStrategyFirstValidResult(list, resultValidator, exception);
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
		public <I, O> CompletionStage<O> sendRequest(I request, int timeout) {
			final SettableStage<O> stage = SettableStage.create();
			FirstResultHandler<O> resultHandler = new FirstResultHandler<>(stage,
					(ResultValidator<O>) resultValidator, subSenders.length, noValidResultException);
			for (RpcSender sender : subSenders) {
				sender.<I, O>sendRequest(request, timeout).whenComplete((o, throwable) -> {
					if (throwable != null) {
						resultHandler.onException(AsyncCallbacks.throwableToException(throwable));
					} else {
						resultHandler.onResult(o);
					}
				});
			}

			return stage;
		}
	}

	private static final class FirstResultHandler<T> {
		private final SettableStage<T> resultStage;
		private final ResultValidator<T> resultValidator;
		private final Exception noValidResultException;
		private int expectedCalls;
		private T result;
		private Exception exception;
		private boolean hasResult;
		private boolean complete;

		public FirstResultHandler(SettableStage<T> resultStage, ResultValidator<T> resultValidator, int expectedCalls,
		                          Exception noValidResultException) {
			checkArgument(expectedCalls > 0);
			this.expectedCalls = expectedCalls;
			this.resultStage = checkNotNull(resultStage);
			this.resultValidator = checkNotNull(resultValidator);
			this.noValidResultException = noValidResultException;
		}

		public void onResult(T value) {
			--expectedCalls;
			if (!hasResult && resultValidator.isValidResult(value)) {
				result = value;  // first valid result
				hasResult = true;
			}
			processResult();
		}

		public void onException(Exception exception) {
			--expectedCalls;
			if (!hasResult) {
				FirstResultHandler.this.exception = exception; // last Exception
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
				resultStage.set(result);
			} else {
				resolveException();
				if (exception == null) {
					resultStage.set(null);
				} else {
					resultStage.setException(exception);
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
