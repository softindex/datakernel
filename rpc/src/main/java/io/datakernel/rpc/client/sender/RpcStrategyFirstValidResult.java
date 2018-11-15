/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Callback;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategyFirstValidResult implements RpcStrategy {
	@FunctionalInterface
	public interface ResultValidator<T> {
		boolean isValidResult(T value);
	}

	private static final ResultValidator<?> DEFAULT_RESULT_VALIDATOR = new DefaultResultValidator<>();

	private final RpcStrategyList list;

	private final ResultValidator<?> resultValidator;
	@Nullable
	private final Exception noValidResultException;

	private RpcStrategyFirstValidResult(RpcStrategyList list, ResultValidator<?> resultValidator,
			@Nullable Exception noValidResultException) {
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

	public RpcStrategyFirstValidResult withNoValidResultException(Exception e) {
		return new RpcStrategyFirstValidResult(list, resultValidator, e);
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return list.getAddresses();
	}

	@Nullable
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
			checkArgument(senders != null && senders.size() > 0, "List of senders should not be null and should contain at least one sender");
			this.subSenders = senders.toArray(new RpcSender[0]);
			this.resultValidator = checkNotNull(resultValidator);
			this.noValidResultException = noValidResultException;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			FirstResultCallback<O> resultCallback
					= new FirstResultCallback<>(cb, (ResultValidator<O>) resultValidator, subSenders.length, noValidResultException);
			for (RpcSender sender : subSenders) {
				sender.sendRequest(request, timeout, resultCallback.getCallback());
			}

		}
	}

	private static final class FirstResultCallback<T> {
		private final Callback<T> resultCallback;
		private final ResultValidator<T> resultValidator;
		private final Exception noValidResultException;
		private int expectedCalls;
		private T result;
		private Throwable exception;
		private boolean hasResult;
		private boolean complete;

		public FirstResultCallback(Callback<T> resultCallback, ResultValidator<T> resultValidator, int expectedCalls,
		                           Exception noValidResultException) {
			checkArgument(expectedCalls > 0, "Number of expected calls should be greater than 0");
			this.expectedCalls = expectedCalls;
			this.resultCallback = checkNotNull(resultCallback);
			this.resultValidator = checkNotNull(resultValidator);
			this.noValidResultException = noValidResultException;
		}

		public Callback<T> getCallback() {
			return new Callback<T>() {
				@Override
				public void set(T result) {
					--expectedCalls;
					if (!hasResult && resultValidator.isValidResult(result)) {
						FirstResultCallback.this.result = result;  // first valid result
						FirstResultCallback.this.hasResult = true;
					}
					processResult();
				}

				@Override
				public void setException(Throwable e) {
					--expectedCalls;
					if (!hasResult) {
						FirstResultCallback.this.exception = e; // last Exception
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
				resultCallback.set(result);
			} else {
				resolveException();
				if (exception == null) {
					resultCallback.set(null);
				} else {
					resultCallback.setException(exception);
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
