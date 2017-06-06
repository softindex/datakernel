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

public final class AsyncResultsReducer<A> extends CompletionCallback {
	private final ResultCallback<A> callback;

	private A accumulator;

	private int activeCallbacks;

	public enum State {RUNNING, FINISHING, FINISHED, ERROR}

	private State state = State.RUNNING;

	private AsyncResultsReducer(ResultCallback<A> callback, A initialAccumulator) {
		this.callback = callback;
		this.accumulator = initialAccumulator;
	}

	public static <A> AsyncResultsReducer<A> create(ResultCallback<A> callback, A initialAccumulator) {
		return new AsyncResultsReducer<>(callback, initialAccumulator);
	}

	public interface ResultReducer<A, V> {
		A applyResult(A accumulator, V value);
	}

	public <V> ResultCallback<V> newResultCallback(final ResultReducer<A, V> reducer) {
		activeCallbacks++;
		return new ResultCallback<V>() {
			@Override
			protected void onResult(V result) {
				if (state == State.RUNNING || state == State.FINISHING) {
					accumulator = reducer.applyResult(accumulator, result);
				}

				if (--activeCallbacks == 0 && state == State.FINISHING) {
					state = State.FINISHED;
					callback.setResult(accumulator);
				}
			}

			@Override
			protected void onException(Exception e) {
				--activeCallbacks;
				if (state == State.RUNNING || state == State.FINISHING) {
					state = State.ERROR;
					callback.setException(e);
				}
			}
		};
	}

	public interface CompletionReducer<A> {
		A applyCompletion(A accumulator);
	}

	public <V> CompletionCallback newCompletionCallback(final CompletionReducer<A> reducer) {
		activeCallbacks++;
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				if (state == State.RUNNING || state == State.FINISHING) {
					accumulator = reducer.applyCompletion(accumulator);
				}

				if (--activeCallbacks == 0 && state == State.FINISHING) {
					state = State.FINISHED;
					callback.setResult(accumulator);
				}
			}

			@Override
			protected void onException(Exception e) {
				--activeCallbacks;
				if (state == State.RUNNING || state == State.FINISHING) {
					state = State.ERROR;
					callback.setException(e);
				}
			}
		};
	}

	@Override
	protected void onComplete() {
		if (state == State.RUNNING) {
			state = State.FINISHING;
		}
		if (activeCallbacks == 0 && state == State.FINISHING) {
			state = State.FINISHED;
			callback.setResult(accumulator);
		}
	}

	@Override
	protected void onException(Exception e) {
		if (state == State.RUNNING || state == State.FINISHING) {
			state = State.ERROR;
			callback.setException(e);
		}
	}

	public int getActiveCallbacks() {
		return activeCallbacks;
	}

	public State getState() {
		return state;
	}
}
