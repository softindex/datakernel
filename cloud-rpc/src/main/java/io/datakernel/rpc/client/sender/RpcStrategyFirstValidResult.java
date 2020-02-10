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

import io.datakernel.async.callback.Callback;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

public final class RpcStrategyFirstValidResult implements RpcStrategy {
	@FunctionalInterface
	public interface ResultValidator<T> {
		boolean isValidResult(T value);
	}

	private static final ResultValidator<?> DEFAULT_RESULT_VALIDATOR = new DefaultResultValidator<>();

	private final RpcStrategyList list;

	private final ResultValidator<?> resultValidator;
	@Nullable
	private final StacklessException noValidResultException;

	private RpcStrategyFirstValidResult(RpcStrategyList list, ResultValidator<?> resultValidator,
			@Nullable StacklessException noValidResultException) {
		this.list = list;
		this.resultValidator = resultValidator;
		this.noValidResultException = noValidResultException;
	}

	public static RpcStrategyFirstValidResult create(RpcStrategyList list) {
		return new RpcStrategyFirstValidResult(list, DEFAULT_RESULT_VALIDATOR, null);
	}

	public RpcStrategyFirstValidResult withResultValidator(@NotNull ResultValidator<?> resultValidator) {
		return new RpcStrategyFirstValidResult(list, resultValidator, noValidResultException);
	}

	public RpcStrategyFirstValidResult withNoValidResultException(@NotNull StacklessException e) {
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
		@Nullable
		private final StacklessException noValidResultException;

		Sender(@NotNull List<RpcSender> senders, @NotNull ResultValidator<?> resultValidator,
				@Nullable StacklessException noValidResultException) {
			assert senders.size() > 0;
			this.subSenders = senders.toArray(new RpcSender[0]);
			this.resultValidator = resultValidator;
			this.noValidResultException = noValidResultException;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
			FirstResultCallback<O> firstResultCallback = new FirstResultCallback<>(subSenders.length, (ResultValidator<O>) resultValidator, cb, noValidResultException);
			for (RpcSender sender : subSenders) {
				sender.sendRequest(request, timeout, firstResultCallback);
			}
		}
	}

	static final class FirstResultCallback<T> implements Callback<T> {
		private int expectedCalls;
		private final ResultValidator<T> resultValidator;
		private final Callback<T> resultCallback;
		private Throwable lastException;
		@Nullable
		private final StacklessException noValidResultException;

		FirstResultCallback(int expectedCalls, @NotNull ResultValidator<T> resultValidator, @NotNull Callback<T> resultCallback,
				@Nullable StacklessException noValidResultException) {
			assert expectedCalls > 0;
			this.expectedCalls = expectedCalls;
			this.resultCallback = resultCallback;
			this.resultValidator = resultValidator;
			this.noValidResultException = noValidResultException;
		}

		@Override
		public void accept(T result, @Nullable Throwable e) {
			if (e == null) {
				if (--expectedCalls >= 0) {
					if (resultValidator.isValidResult(result)) {
						expectedCalls = 0;
						resultCallback.accept(result, null);
					} else {
						if (expectedCalls == 0) {
							resultCallback.accept(null, lastException != null ? lastException : noValidResultException);
						}
					}
				}
			} else {
				lastException = e; // last Exception
				if (--expectedCalls == 0) {
					resultCallback.accept(null, lastException);
				}
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
