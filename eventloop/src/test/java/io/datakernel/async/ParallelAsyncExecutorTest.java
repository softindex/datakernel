package io.datakernel.async;

import io.datakernel.eventloop.NioEventloop;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static org.junit.Assert.assertEquals;

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
		NioEventloop eventloop = new NioEventloop();
		Map<Integer, ExecutionInfo> executionInfoMap = new HashMap<>();
		int tasks = 5;

		for (int i = 0; i < tasks; ++i) {
			submitTestTask(executor, eventloop, executionInfoMap, i);
		}

		assertEquals(1, eventloop.getScheduledRunnables());

		eventloop.run();

		for (int i = 1; i < tasks; ++i) {
			ExecutionInfo previous = executionInfoMap.get(i - 1);
			ExecutionInfo current = executionInfoMap.get(i);
			assertEquals(previous.endTimestamp, current.startTimestamp);
		}
	}

	@Test
	public void testParallel() throws Exception {
		int maxParallelism = 3;
		AsyncExecutor executor = AsyncExecutors.parallelExecutor(maxParallelism, 5);
		NioEventloop eventloop = new NioEventloop();
		Map<Integer, ExecutionInfo> executionInfoMap = new HashMap<>();
		int tasks = 9;

		for (int i = 0; i < tasks; ++i) {
			submitTestTask(executor, eventloop, executionInfoMap, i);
		}

		assertEquals(true, executor.isSaturated());
		assertEquals(maxParallelism, eventloop.getScheduledRunnables());

		eventloop.run();

		for (int j = 0; j < tasks; j += maxParallelism) {
			for (int i = 1; i < maxParallelism; ++i) {
				ExecutionInfo previous = executionInfoMap.get(i + j - 1);
				ExecutionInfo current = executionInfoMap.get(i + j);
				assertEquals(previous.startTimestamp, current.startTimestamp);
			}
		}
	}

	private static void submitTestTask(AsyncExecutor executor, NioEventloop eventloop, Map<Integer, ExecutionInfo> executionInfoMap, int n) {
		executor.submit(getTestTask(eventloop, n, executionInfoMap), ignoreCompletionCallback());
	}

	private static AsyncTask getTestTask(final NioEventloop eventloop, final int n, final Map<Integer, ExecutionInfo> executionInfoMap) {
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