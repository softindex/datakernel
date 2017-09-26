package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class Stages {
	private Stages() {
	}

	public static <T> CompletionStage<T> timeout(Eventloop eventloop, CompletionStage<T> stage, long timestampMillis) {
		SettableStage<T> resultStage = SettableStage.create();
		ScheduledRunnable schedule = eventloop.schedule(timestampMillis, () -> resultStage.setException(new TimeoutException()));
		stage.whenComplete((result, throwable) -> {
			schedule.cancel();
			if (!resultStage.isComplete()) {
				if (throwable == null) {
					resultStage.set(result);
				} else {
					resultStage.setException(throwable);
				}
			}
		});
		return resultStage;
	}

	private static final class Counter {
		int counter;

		public Counter(int counter) {
			this.counter = counter;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> CompletionStage<T[]> all(CompletionStage<? extends T>... stages) {
		if (stages.length == 0)
			return SettableStage.immediateStage((T[]) new Object[0]);
		if (stages.length == 1) {
			return stages[0].thenApply(t -> (T[]) new Object[]{t});
		}
		SettableStage<T[]> resultStage = SettableStage.create();
		T[] results = (T[]) new Object[stages.length];
		Counter counter = new Counter(stages.length);
		for (int i = 0; i < stages.length; i++) {
			int finalI = i;
			stages[i].whenComplete((result, throwable) -> {
				if (throwable == null) {
					if (!resultStage.isSet()) {
						results[finalI] = result;
						if (--counter.counter == 0) {
							resultStage.set(results);
						}
					}
				} else {
					resultStage.trySetException(throwable);
				}
			});
		}
		return resultStage;
	}

	@SuppressWarnings("unchecked")
	public static <T> CompletionStage<List<T>> all(List<? extends CompletionStage<T>> stages) {
		return all(stages.toArray((CompletionStage<T>[]) new CompletionStage[stages.size()])).thenApply(Arrays::asList);
	}

	public static void tryCancel(CompletionStage<?> stage) {
		if (stage instanceof AsyncCancellable) {
			((AsyncCancellable) stage).cancel();
		}
	}

	public static CompletionStage<Void> sequence(Iterable<StageRunnable> stages) {
		return sequence(stages.iterator());
	}

	public static CompletionStage<Void> sequence(Iterator<StageRunnable> stages) {
		if (!stages.hasNext()) {
			return SettableStage.immediateStage(null);
		}
		StageRunnable next = stages.next();
		return next.run().thenCompose($ -> sequence(stages));
	}

	public static final class Tuple2<V1, V2> {
		private int counter = 2;
		private V1 value1;
		private V2 value2;

		private Tuple2() {
		}

		public V1 getValue1() {
			return value1;
		}

		public V2 getValue2() {
			return value2;
		}
	}

	@SuppressWarnings("unchecked")
	public static <V1, V2> CompletionStage<Tuple2<V1, V2>> tuple(CompletionStage<V1> stage1, CompletionStage<V2> stage2) {
		SettableStage<Tuple2<V1, V2>> resultStage = SettableStage.create();
		Tuple2<V1, V2> results = new Tuple2<>();
		stage1.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (!resultStage.isSet()) {
					results.value1 = result;
					if (--results.counter == 0) {
						resultStage.set(results);
					}
				}
			} else {
				resultStage.trySetException(throwable);
			}
		});
		stage2.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (!resultStage.isSet()) {
					results.value2 = result;
					if (--results.counter == 0) {
						resultStage.set(results);
					}
				}
			} else {
				resultStage.trySetException(throwable);
			}
		});
		return resultStage;
	}

	public static final class Tuple3<V1, V2, V3> {
		private int counter = 3;
		private V1 value1;
		private V2 value2;
		private V3 value3;

		private Tuple3() {
		}

		public V1 getValue1() {
			return value1;
		}

		public V2 getValue2() {
			return value2;
		}

		public V3 getValue3() {
			return value3;
		}
	}

	@SuppressWarnings("unchecked")
	public static <V1, V2, V3> CompletionStage<Tuple3<V1, V2, V3>> tuple(CompletionStage<V1> stage1, CompletionStage<V2> stage2, CompletionStage<V3> stage3) {
		SettableStage<Tuple3<V1, V2, V3>> resultStage = SettableStage.create();
		Tuple3<V1, V2, V3> results = new Tuple3<>();
		stage1.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (!resultStage.isSet()) {
					results.value1 = result;
					if (--results.counter == 0) {
						resultStage.set(results);
					}
				}
			} else {
				resultStage.trySetException(throwable);
			}
		});
		stage2.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (!resultStage.isSet()) {
					results.value2 = result;
					if (--results.counter == 0) {
						resultStage.set(results);
					}
				}
			} else {
				resultStage.trySetException(throwable);
			}
		});
		stage3.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (!resultStage.isSet()) {
					results.value3 = result;
					if (--results.counter == 0) {
						resultStage.set(results);
					}
				}
			} else {
				resultStage.trySetException(throwable);
			}
		});
		return resultStage;
	}

	public static <T> BiConsumer<T, ? super Throwable> assertBiConsumer(Consumer<T> consumer) {
		return (BiConsumer<T, Throwable>) (t, throwable) -> {
			if (throwable != null)
				throw new AssertionError("Fatal error in bi consumer", throwable);
			consumer.accept(t);
		};
	}

}
