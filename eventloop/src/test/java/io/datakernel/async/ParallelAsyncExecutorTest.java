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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ParallelAsyncExecutorTest {
	private static class ExecutionInfo {
		private final long startTimestamp;
		private final long endTimestamp;

		public ExecutionInfo(long startTimestamp, long endTimestamp) {
			this.startTimestamp = startTimestamp;
			this.endTimestamp = endTimestamp;
		}
	}

	@Test
	public void testSequential() throws Exception {
		AsyncExecutor executor = AsyncExecutors.sequentialExecutor();
		Eventloop eventloop = Eventloop.create();
		Map<Integer, ExecutionInfo> executionInfoMap = new HashMap<>();
		int tasks = 5;

		for (int i = 0; i < tasks; ++i) {
			submitTestTask(executor, eventloop, executionInfoMap, i);
		}

		assertEquals(1, eventloop.getCurrentScheduledTasks());

		eventloop.run();

		for (int i = 1; i < tasks; ++i) {
			ExecutionInfo previous = executionInfoMap.get(i - 1);
			ExecutionInfo current = executionInfoMap.get(i);
			assertEquals(previous.endTimestamp, current.startTimestamp);
		}
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testParallel() throws Exception {
		int maxParallelism = 3;
		AsyncExecutor executor = AsyncExecutors.parallelExecutor(maxParallelism, 5);
		Eventloop eventloop = Eventloop.create();
		Map<Integer, ExecutionInfo> executionInfoMap = new HashMap<>();
		int tasks = 9;

		for (int i = 0; i < tasks; ++i) {
			submitTestTask(executor, eventloop, executionInfoMap, i);
		}

		assertEquals(true, executor.isSaturated());
		assertEquals(maxParallelism, eventloop.getCurrentScheduledTasks());

		eventloop.run();

		for (int j = 0; j < tasks; j += maxParallelism) {
			for (int i = 1; i < maxParallelism; ++i) {
				ExecutionInfo previous = executionInfoMap.get(i + j - 1);
				ExecutionInfo current = executionInfoMap.get(i + j);
				assertEquals(previous.startTimestamp, current.startTimestamp);
			}
		}
		assertThat(eventloop, doesntHaveFatals());
	}

	private static void submitTestTask(AsyncExecutor executor, Eventloop eventloop, Map<Integer, ExecutionInfo> executionInfoMap, int n) {
		executor.submit(getTestTask(eventloop, n, executionInfoMap), ignoreCompletionCallback());
	}

	private static AsyncTask getTestTask(final Eventloop eventloop, final int n, final Map<Integer, ExecutionInfo> executionInfoMap) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				final long startTimestamp = eventloop.currentTimeMillis();
				eventloop.schedule(eventloop.currentTimeMillis() + 10, new Runnable() {
					@Override
					public void run() {
						long endTimestamp = eventloop.currentTimeMillis();
						executionInfoMap.put(n, new ExecutionInfo(startTimestamp, endTimestamp));
						callback.onComplete();
					}
				});
			}
		};
	}
}