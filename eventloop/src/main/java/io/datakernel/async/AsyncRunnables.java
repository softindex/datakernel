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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.AsyncTimeoutException;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import static java.util.Arrays.asList;

public class AsyncRunnables {
	public static final AsyncTimeoutException RUNNABLE_TIMEOUT_EXCEPTION =
			new AsyncTimeoutException("AsyncRunnable timeout");

	private AsyncRunnables() {
	}

	public static AsyncRunnable timeout(final Eventloop eventloop, final long timestamp, final AsyncRunnable runnable) {
		return () -> {
			final SettableStage<Void> stage = SettableStage.create();
			final CompletionStage<Void> stageRun = runnable.run();
			final ScheduledRunnable scheduledRunnable = eventloop.schedule(timestamp, () -> {
				stage.setException(RUNNABLE_TIMEOUT_EXCEPTION);
				Stages.tryCancel(stageRun);
			});

			stageRun.whenComplete(($, throwable) -> {
				if (scheduledRunnable.isComplete()) return;
				scheduledRunnable.cancel();
				stage.set(null, throwable);
			});

			return stage;
		};
	}

	public static AsyncRunnable runInSequence(final Eventloop eventloop, final AsyncRunnable... runnables) {
		return runInSequence(eventloop, asList(runnables));
	}

	public static AsyncRunnable runInSequence(final Eventloop eventloop, final Iterable<? extends AsyncRunnable> runnables) {
		return new AsyncRunnable() {
			@Override
			public CompletionStage<Void> run() {
				final SettableStage<Void> stage = SettableStage.create();
				next(runnables.iterator(), stage);
				return stage;
			}

			void next(final Iterator<? extends AsyncRunnable> iterator, SettableStage<Void> stage) {
				if (iterator.hasNext()) {
					AsyncRunnable nextTask = iterator.next();
					final long microTick = eventloop.getMicroTick();
					nextTask.run().whenComplete(($, throwable) -> {
						if (throwable == null) {
							if (eventloop.getMicroTick() != microTick) {
								next(iterator, stage);
							} else {
								eventloop.post(() -> next(iterator, stage));
							}
						} else {
							stage.setException(throwable);
						}
					});
				} else {
					stage.set(null);
				}
			}
		};
	}

	public static AsyncRunnable runInParallel(final Eventloop eventloop, final AsyncRunnable... runnables) {
		return runInParallel(eventloop, asList(runnables));
	}

	private static final class RunState {
		int pending;

		public RunState(int pending) {
			this.pending = pending;
		}
	}

	public static AsyncRunnable runInParallel(final Eventloop eventloop, final Collection<? extends AsyncRunnable> runnables) {
		return () -> {
			final SettableStage<Void> stage = SettableStage.create();
			final RunState state = new RunState(runnables.size());
			if (state.pending == 0) {
				stage.postResult(eventloop, null);
				return stage;
			}

			for (AsyncRunnable runnable : runnables) {
				runnable.run().whenComplete(($, throwable) -> {
					if (throwable == null) {
						if (--state.pending == 0) {
							stage.set(null);
						}
					} else {
						if (state.pending > 0) {
							state.pending = 0;
							stage.setException(throwable);
						}
					}
				});
			}

			return stage;
		};
	}
}
