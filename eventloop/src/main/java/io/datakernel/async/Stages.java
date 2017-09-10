package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

public class Stages {
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
					if (!resultStage.isSet()) {
						resultStage.setException(throwable);
					}
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
		return next.run().thenAccept($ -> sequence(stages));
	}

}
