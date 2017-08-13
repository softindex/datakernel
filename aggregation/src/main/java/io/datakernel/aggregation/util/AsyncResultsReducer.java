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

package io.datakernel.aggregation.util;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

public final class AsyncResultsReducer<A> {
	private ResultCallback<A> callback;

	private A accumulator;
	private Exception exception;

	private int activeResultCallbacks;
	private int activeCompletionCallbacks;

	private AsyncResultsReducer(A initialAccumulator) {
		this.accumulator = initialAccumulator;
	}

	public static <A> AsyncResultsReducer<A> create(A initialAccumulator) {
		return new AsyncResultsReducer<>(initialAccumulator);
	}

	public AsyncResultsReducer<A> withResultCallback(ResultCallback<A> callback) {
		this.callback = callback;
		return this;
	}

	public interface ResultReducer<A, V> {
		A applyResult(A accumulator, V value);
	}

	public <V> ResultCallback<V> newResultCallback(final ResultReducer<A, V> reducer) {
		activeResultCallbacks++;
		return new ResultCallback<V>() {
			@Override
			protected void onResult(V result) {
				if (accumulator != null) {
					accumulator = reducer.applyResult(accumulator, result);
					assert accumulator != null;
				}

				activeResultCallbacks--;
				if ((activeResultCallbacks + activeCompletionCallbacks) == 0 && callback != null && exception == null) {
					callback.setResult(accumulator);
					callback = null;
				}
			}

			@Override
			protected void onException(Exception e) {
				activeResultCallbacks--;
				if (exception == null) {
					exception = e;
					accumulator = null;
					if (callback != null) {
						callback.setException(e);
						callback = null;
					}
				}
			}
		};
	}

	public interface CompletionReducer<A> {
		A applyCompletion(A accumulator);
	}

	public CompletionCallback newCompletionCallback() {
		return newCompletionCallback(new CompletionReducer<A>() {
			@Override
			public A applyCompletion(A accumulator) {
				return accumulator;
			}
		});
	}

	public CompletionCallback newCompletionCallback(final CompletionReducer<A> reducer) {
		activeCompletionCallbacks++;
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				if (exception != null) {
					accumulator = reducer.applyCompletion(accumulator);
				}

				activeCompletionCallbacks--;
				if ((activeResultCallbacks + activeCompletionCallbacks) == 0 && callback != null && exception == null) {
					callback.setResult(accumulator);
					callback = null;
				}
			}

			@Override
			protected void onException(Exception e) {
				--activeCompletionCallbacks;
				if (exception == null) {
					exception = e;
					if (callback != null) {
						callback.setException(e);
						callback = null;
					}
				}
			}
		};
	}

	public void setResultTo(ResultCallback<A> callback) {
		assert this.callback == null;
		if (exception != null) {
			callback.setException(exception);
			return;
		}
		if ((activeResultCallbacks + activeCompletionCallbacks) == 0) {
			callback.setResult(accumulator);
			return;
		}
		this.callback = callback;
	}

	public int getActiveCallbacks() {
		return activeResultCallbacks + activeCompletionCallbacks;
	}

	public int getActiveResultCallbacks() {
		return activeResultCallbacks;
	}

	public int getActiveCompletionCallbacks() {
		return activeCompletionCallbacks;
	}

	public Exception getException() {
		return exception;
	}

	public A getAccumulator() {
		return accumulator;
	}
}
