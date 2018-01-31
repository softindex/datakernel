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

package io.datakernel.async;

public final class StagesAccumulator<A> {
	private SettableStage<A> resultStage;

	private A accumulator;
	private Throwable exception;

	private int activeStages;

	private StagesAccumulator(A initialAccumulator) {
		this.accumulator = initialAccumulator;
	}

	public static <A> StagesAccumulator<A> create(A initialAccumulator) {
		return new StagesAccumulator<>(initialAccumulator);
	}

	public <V> StagesAccumulator<A> withStage(Stage<V> stage, Reducer<A, V> reducer) {
		addStage(stage, reducer);
		return this;
	}

	public <V> Stage<V> addStage(Stage<V> stage, Reducer<A, V> reducer) {
		activeStages++;
		return stage.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (accumulator != null) {
					reducer.accumulate(accumulator, result);
					assert accumulator != null;
				}

				activeStages--;
				if (activeStages == 0 && resultStage != null && exception == null) {
					resultStage.set(accumulator);
					resultStage = null;
				}
			} else {
				activeStages--;
				if (exception == null) {
					exception = throwable;
					accumulator = null;
					if (resultStage != null) {
						resultStage.setException(throwable);
						resultStage = null;
					}
				}
			}
		});
	}

	public <V> SettableStage<V> newStage(Reducer<A, V> reducer) {
		SettableStage<V> resultStage = SettableStage.create();
		addStage(resultStage, reducer);
		return resultStage;
	}

	public Stage<A> get() {
		if (resultStage != null)
			return resultStage;
		resultStage = SettableStage.create();
		if (exception != null) {
			resultStage.setException(exception);
			return resultStage;
		}
		if (activeStages == 0) {
			resultStage.set(accumulator);
			return resultStage;
		}
		return resultStage;
	}

	public A getAccumulator() {
		return accumulator;
	}

	public Throwable getException() {
		return exception;
	}

	public int getActiveStages() {
		return activeStages;
	}
}
