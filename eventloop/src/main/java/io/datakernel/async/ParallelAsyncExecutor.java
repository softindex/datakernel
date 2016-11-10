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

import java.util.ArrayDeque;
import java.util.Deque;

public final class ParallelAsyncExecutor implements AsyncExecutor {
	private final Deque<AsyncRunnable> taskQueue = new ArrayDeque<>();
	private final Deque<CompletionCallback> callbackQueue = new ArrayDeque<>();
	private final int maxParallelism;
	private final int queueSaturationThreshold;

	private int executing;

	// region builders
	private ParallelAsyncExecutor(int maxParallelism, int queueSaturationThreshold) {
		this.maxParallelism = maxParallelism;
		this.queueSaturationThreshold = queueSaturationThreshold;
	}

	public static ParallelAsyncExecutor create(int maxParallelism) {
		return new ParallelAsyncExecutor(maxParallelism, Integer.MAX_VALUE);
	}

	public ParallelAsyncExecutor withQueueSaturationThreshhold(int queueSaturationThreshhold) {
		return new ParallelAsyncExecutor(maxParallelism, queueSaturationThreshhold);
	}
	// endregion

	@Override
	public void submit(AsyncRunnable asyncRunnable, CompletionCallback callback) {
		if (taskQueue.isEmpty() && executing < maxParallelism) {
			++executing;
			asyncRunnable.run(getInternalCallback(callback));
		} else {
			taskQueue.add(asyncRunnable);
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
			protected void onComplete() {
				--executing;
				realCallback.setComplete();
				executeNextTask();
			}

			@Override
			protected void onException(Exception exception) {
				--executing;
				realCallback.setException(exception);
				executeNextTask();
			}
		};
	}

	private void executeNextTask() {
		if (!taskQueue.isEmpty()) {
			AsyncRunnable queuedTask = taskQueue.pollFirst();
			CompletionCallback queuedCallback = callbackQueue.pollFirst();
			++executing;
			queuedTask.run(getInternalCallback(queuedCallback));
		}
	}
}
