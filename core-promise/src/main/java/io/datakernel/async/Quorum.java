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

package io.datakernel.async;

import io.datakernel.exception.StacklessException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public final class Quorum {
	private Quorum() {
		throw new AssertionError("nope.");
	}

	public static <T, R> Function<T, Promise<R>> create(Iterator<Function<T, Promise<R>>> functions, Function<List<R>, R> reducer, int requiredSuccesses, int maxParallelCalls) {
		return new Function<T, Promise<R>>() {
			int running;
			List<R> successes = new ArrayList<>();
			List<Throwable> failures = new ArrayList<>();
			SettablePromise<R> result = new SettablePromise<>();

			private void call(T t) {
				while (running < maxParallelCalls) {
					if (!functions.hasNext()) {
						if (running == 0 && !result.isComplete()) {
							Exception exception = new StacklessException(Quorum.class, "Not enough successful completions, " +
									requiredSuccesses + " were required, only " + successes.size() + " succeeded");
							failures.forEach(exception::addSuppressed);
							result.setException(exception);
						}
						return;
					}
					running++;
					functions.next()
							.apply(t)
							.acceptEx((res, e) -> {
								if (e != null) {
									failures.add(e);
								} else if (successes.size() < requiredSuccesses) {
									successes.add(res);
									if (successes.size() == requiredSuccesses) {
										result.set(reducer.apply(successes));
										return;
									}
								}
								running--;
								call(t);
							});
				}
			}

			@Override
			public Promise<R> apply(T t) {
				call(t);
				return result;
			}
		};
	}
}
