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

import io.datakernel.async.SettableStage;

import java.util.concurrent.CompletionStage;

public final class AsyncResultsReducer<A> {
	private SettableStage<A> resultStage;

	private A accumulator;
	private Throwable error;

	private int activeStages;

	private AsyncResultsReducer(A initialAccumulator) {
		this.accumulator = initialAccumulator;
	}

	public static <A> AsyncResultsReducer<A> create(A initialAccumulator) {
		return new AsyncResultsReducer<>(initialAccumulator);
	}

	public <V> AsyncResultsReducer<A> withStage(CompletionStage<V> stage, final ResultReducer<A, V> reducer) {
		addStage(stage, reducer);
		return this;
	}

	public interface ResultReducer<A, V> {
		A applyResult(A accumulator, V value);
	}

	public <V> void addStage(CompletionStage<V> stage, final ResultReducer<A, V> reducer) {
		activeStages++;
		stage.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (accumulator != null) {
					accumulator = reducer.applyResult(accumulator, result);
					assert accumulator != null;
				}

				activeStages--;
				if (activeStages == 0 && resultStage != null && error == null) {
					resultStage.setResult(accumulator);
					resultStage = null;
				}
			} else {
				activeStages--;
				if (error == null) {
					error = throwable;
					accumulator = null;
					if (resultStage != null) {
						resultStage.setError(throwable);
						resultStage = null;
					}
				}
			}
		});
	}

	public CompletionStage<A> getResult() {
		assert resultStage == null;
		resultStage = SettableStage.create();
		if (error != null) {
			resultStage.setError(error);
			return resultStage;
		}
		if (activeStages == 0) {
			resultStage.setResult(accumulator);
			return resultStage;
		}
		return resultStage;
	}

	public int getActiveStages() {
		return activeStages;
	}
}
