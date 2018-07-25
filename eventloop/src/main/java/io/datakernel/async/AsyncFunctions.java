package io.datakernel.async;

import java.util.HashMap;
import java.util.Map;

public class AsyncFunctions {
	private AsyncFunctions() {}

	public static <T, R> AsyncFunction<T, R> reuse(AsyncFunction<? super T, ? extends R> actual) {
		return new AsyncFunction<T, R>() {
			private final Map<T, Stage<R>> runningStages = new HashMap<>();

			@SuppressWarnings("unchecked")
			@Override
			public Stage<R> apply(T input) {
				Stage<R> runningStage = runningStages.get(input);
				if (runningStage == null) {
					runningStage = (Stage<R>) actual.apply(input);
					runningStage.whenComplete((result, throwable) -> runningStages.remove(input));
					runningStages.put(input, runningStage);
				}
				return runningStage;
			}
		};
	}

	private static final Object NO_VALUE = new Object();

	public static <T, R> AsyncFunction<T, R> memoize(AsyncFunction<? super T, ? extends R> actual) {
		return new AsyncFunction<T, R>() {
			private final Map<T, R> memoizedResults = new HashMap<>();
			private final Map<T, Stage<R>> runningStages = new HashMap<>();

			@SuppressWarnings("unchecked")
			@Override
			public Stage<R> apply(T input) {
				R memoizedResult = memoizedResults.getOrDefault(input, (R) NO_VALUE);
				if (memoizedResult != NO_VALUE) return Stage.of(memoizedResult);

				Stage<R> runningStage = runningStages.get(input);
				if (runningStage != null) return runningStage;

				runningStage = (Stage<R>) actual.apply(input);
				runningStage.whenComplete((result, throwable) -> {
					if (throwable == null) {
						memoizedResults.put(input, result);
					}
					runningStages.remove(input);
				});
				runningStages.put(input, runningStage);
				return runningStage;
			}
		};
	}

	public static <T, R> AsyncFunction<T, R> buffered(AsyncFunction<T, R> actual) {
		return buffered(1, Integer.MAX_VALUE, actual);
	}

	@SuppressWarnings("unchecked")
	public static <T, R> AsyncFunction<T, R> buffered(int maxParallelCalls, int maxBufferedCalls, AsyncFunction<T, R> asyncFunction) {
		return asyncFunction.withExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls));
	}

}
