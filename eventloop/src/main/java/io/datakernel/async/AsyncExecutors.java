package io.datakernel.async;

public final class AsyncExecutors {
	public static AsyncExecutor sequentialExecutor() {
		return new ParallelAsyncExecutor(1);
	}

	public static AsyncExecutor sequentialExecutor(int queueSaturationThreshold) {
		return new ParallelAsyncExecutor(1, queueSaturationThreshold);
	}

	public static AsyncExecutor parallelExecutor(int maxParallelism) {
		return new ParallelAsyncExecutor(maxParallelism);
	}

	public static AsyncExecutor parallelExecutor(int maxParallelism, int queueSaturationThreshold) {
		return new ParallelAsyncExecutor(maxParallelism, queueSaturationThreshold);
	}

	public static AsyncExecutor immediateExecutor() {
		return new AsyncExecutor() {
			@Override
			public void submit(AsyncTask asyncTask, CompletionCallback callback) {
				asyncTask.execute(callback);
			}

			@Override
			public boolean isSaturated() {
				return false;
			}
		};
	}
}
