package io.datakernel.async;

import java.util.ArrayDeque;
import java.util.Deque;

public final class ParallelAsyncExecutor implements AsyncExecutor {
	private final Deque<AsyncTask> taskQueue = new ArrayDeque<>();
	private final Deque<CompletionCallback> callbackQueue = new ArrayDeque<>();
	private final int maxParallelism;
	private final int queueSaturationThreshold;

	private int executing;

	public ParallelAsyncExecutor(int maxParallelism) {
		this(maxParallelism, Integer.MAX_VALUE);
	}

	public ParallelAsyncExecutor(int maxParallelism, int queueSaturationThreshold) {
		this.maxParallelism = maxParallelism;
		this.queueSaturationThreshold = queueSaturationThreshold;
	}

	@Override
	public void submit(AsyncTask asyncTask, CompletionCallback callback) {
		if (taskQueue.isEmpty() && executing < maxParallelism) {
			++executing;
			asyncTask.execute(getInternalCallback(callback));
		} else {
			taskQueue.add(asyncTask);
			callbackQueue.add(callback);
		}
	}

	@Override
	public boolean isSaturated() {
		return taskQueue.size() > queueSaturationThreshold;
	}

	private CompletionCallback getInternalCallback(final CompletionCallback realCallback) {
		return new CompletionCallback() {
			@Override
			public void onComplete() {
				--executing;
				realCallback.onComplete();
				executeNextTask();
			}

			@Override
			public void onException(Exception exception) {
				--executing;
				realCallback.onException(exception);
				executeNextTask();
			}
		};
	}

	private void executeNextTask() {
		if (!taskQueue.isEmpty()) {
			AsyncTask queuedTask = taskQueue.pollFirst();
			CompletionCallback queuedCallback = callbackQueue.pollFirst();
			queuedTask.execute(getInternalCallback(queuedCallback));
		}
	}
}
