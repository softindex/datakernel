package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;

public class Stages {
	private Stages() {
	}

	public static <T> CompletionStage<T> timeout(Eventloop eventloop, CompletionStage<T> stage, long timestampMillis) {
		SettableStage<T> resultStage = SettableStage.create();
		ScheduledRunnable schedule = eventloop.schedule(timestampMillis, () -> resultStage.setError(new TimeoutException()));
		stage.whenComplete((result, throwable) -> {
			schedule.cancel();
			if (!resultStage.isComplete()) {
				if (throwable == null) {
					resultStage.setResult(result);
				} else {
					resultStage.setError(throwable);
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

	public static <T> CompletionStage<List<T>> all(List<CompletionStage<T>> stages) {
		if (stages.size() == 0)
			return SettableStage.immediateStage(emptyList());
		if (stages.size() == 1) {
			return stages.get(0).thenApply(Collections::singletonList);
		}
		SettableStage<List<T>> resultStage = SettableStage.create();
		List<T> list = new ArrayList<>(stages.size());
		Counter counter = new Counter(stages.size());
		for (int i = 0; i < stages.size(); i++) {
			list.add(null);
			int finalI = i;
			stages.get(i).whenComplete((result, throwable) -> {
				if (throwable == null) {
					if (!resultStage.isDone()) {
						list.set(finalI, result);
						if (--counter.counter == 0) {
							resultStage.setResult(list);
						}
					}
				} else {
					list.clear();
					if (!resultStage.isDone()) {
						resultStage.setError(throwable);
					}
				}
			});
		}
		return resultStage;
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
