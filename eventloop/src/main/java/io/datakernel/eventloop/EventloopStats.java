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

package io.datakernel.eventloop;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.*;
import io.datakernel.util.Stopwatch;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

public final class EventloopStats {
	private static final class DurationRunnable {
		private Runnable runnable;
		private long duration;

		void reset() {
			duration = 0;
			runnable = null;
		}

		void update(Runnable runnable, long duration) {
			this.duration = duration;
			this.runnable = runnable;
		}

		long getDuration() {
			return duration;
		}

		@Override
		public String toString() {
			return (runnable == null) ? "" : runnable.getClass().getName() + ": " + duration;
		}
	}

	private static final class StackTrace {
		private final StackTraceElement[] stackTraceElements;

		public StackTrace(StackTraceElement[] stackTraceElements) {
			this.stackTraceElements = stackTraceElements;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof StackTrace)) return false;

			StackTrace that = (StackTrace) o;

			return Arrays.equals(stackTraceElements, that.stackTraceElements);
		}

		@Override
		public int hashCode() {
			return stackTraceElements != null ? Arrays.hashCode(stackTraceElements) : 0;
		}
	}

	private final ValueStats selectorSelectTime;
	private final ValueStats businessLogicTime;
	private final EventStats selectedKeys;
	private final EventStats invalidKeys;
	private final EventStats acceptKeys;
	private final EventStats connectKeys;
	private final EventStats readKeys;
	private final EventStats writeKeys;
	private final EventStats localTasks;
	private final EventStats concurrentTasks;
	private final EventStats scheduledTasks;

	private final ValueStats localTaskDuration;
	private final DurationRunnable lastLongestLocalRunnable;
	private final ValueStats concurrentTaskDuration;
	private final DurationRunnable lastLongestConcurrentRunnable;
	private final ValueStats scheduledTaskDuration;
	private final DurationRunnable lastLongestScheduledRunnable;

	private final ValueStats selectedKeysTime;
	private final ValueStats localTasksTime;
	private final ValueStats concurrentTasksTime;
	private final ValueStats scheduledTasksTime;

	private final ExceptionStats fatalErrors;
	private final Map<StackTrace, ExceptionStats> allFatalErrors;
	private final ExceptionStats ioErrors;

	private final BusinessLogicTimeHistogram businessLogicTimeHistogram
			= new BusinessLogicTimeHistogram();

	public EventloopStats(double smoothingWindow) {
		selectorSelectTime = new ValueStats(smoothingWindow);
		businessLogicTime = new ValueStats(smoothingWindow);
		selectedKeys = new EventStats(smoothingWindow);
		invalidKeys = new EventStats(smoothingWindow);
		acceptKeys = new EventStats(smoothingWindow);
		connectKeys = new EventStats(smoothingWindow);
		readKeys = new EventStats(smoothingWindow);
		writeKeys = new EventStats(smoothingWindow);
		localTasks = new EventStats(smoothingWindow);
		concurrentTasks = new EventStats(smoothingWindow);
		scheduledTasks = new EventStats(smoothingWindow);

		localTaskDuration = new ValueStats(smoothingWindow);
		lastLongestLocalRunnable = new DurationRunnable();
		concurrentTaskDuration = new ValueStats(smoothingWindow);
		lastLongestConcurrentRunnable = new DurationRunnable();
		scheduledTaskDuration = new ValueStats(smoothingWindow);
		lastLongestScheduledRunnable = new DurationRunnable();

		selectedKeysTime = new ValueStats(smoothingWindow);
		localTasksTime = new ValueStats(smoothingWindow);
		concurrentTasksTime = new ValueStats(smoothingWindow);
		scheduledTasksTime = new ValueStats(smoothingWindow);

		fatalErrors = new ExceptionStats();
		allFatalErrors = new HashMap<>();
		ioErrors = new ExceptionStats();
	}

	public void setSmoothingWindow(double smoothingWindow) {
		selectorSelectTime.setSmoothingWindow(smoothingWindow);
		businessLogicTime.setSmoothingWindow(smoothingWindow);
		selectedKeys.setSmoothingWindow(smoothingWindow);
		invalidKeys.setSmoothingWindow(smoothingWindow);
		acceptKeys.setSmoothingWindow(smoothingWindow);
		connectKeys.setSmoothingWindow(smoothingWindow);
		readKeys.setSmoothingWindow(smoothingWindow);
		writeKeys.setSmoothingWindow(smoothingWindow);
		localTasks.setSmoothingWindow(smoothingWindow);
		concurrentTasks.setSmoothingWindow(smoothingWindow);
		scheduledTasks.setSmoothingWindow(smoothingWindow);

		localTaskDuration.setSmoothingWindow(smoothingWindow);
		concurrentTaskDuration.setSmoothingWindow(smoothingWindow);
		scheduledTaskDuration.setSmoothingWindow(smoothingWindow);

		selectedKeysTime.setSmoothingWindow(smoothingWindow);
		localTasksTime.setSmoothingWindow(smoothingWindow);
		concurrentTasksTime.setSmoothingWindow(smoothingWindow);
		scheduledTasksTime.setSmoothingWindow(smoothingWindow);
	}

	public void updateBusinessLogicTime(long businessLogicTime) {
		this.businessLogicTime.recordValue((int) businessLogicTime);
		businessLogicTimeHistogram.update(businessLogicTime);
	}

	public void updateSelectorSelectTime(long selectorSelectTime) {
		this.selectorSelectTime.recordValue((int) selectorSelectTime);
	}

	public void updateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys) {
		this.selectedKeys.recordEvents(lastSelectedKeys);
		this.invalidKeys.recordEvents(invalidKeys);
		this.acceptKeys.recordEvents(acceptKeys);
		this.connectKeys.recordEvents(connectKeys);
		this.readKeys.recordEvents(readKeys);
		this.writeKeys.recordEvents(writeKeys);
	}

	public void updateSelectedKeysTimeStats(@Nullable Stopwatch sw) {
		if (sw != null)
			selectedKeysTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
	}

	private void updateTaskDuration(ValueStats counter, DurationRunnable longestCounter, Runnable runnable, @Nullable Stopwatch sw) {
		if (sw != null) {
			int elapsed = (int) sw.elapsed(TimeUnit.MICROSECONDS);
			counter.recordValue(elapsed);
			if (elapsed > longestCounter.getDuration()) {
				longestCounter.update(runnable, elapsed);
			}
		}
	}

	public void updateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(localTaskDuration, lastLongestLocalRunnable, runnable, sw);
	}

	public void updateLocalTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			localTasksTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		localTasks.recordEvents(newTasks);
	}

	public void updateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(concurrentTaskDuration, lastLongestConcurrentRunnable, runnable, sw);
	}

	public void updateConcurrentTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			concurrentTasksTime.recordValue((int) sw.elapsed(TimeUnit.MICROSECONDS));
		concurrentTasks.recordEvents(newTasks);
	}

	public void updateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(scheduledTaskDuration, lastLongestScheduledRunnable, runnable, sw);
	}

	public void updateScheduledTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			scheduledTasksTime.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		scheduledTasks.recordEvents(newTasks);
	}

	public void recordFatalError(Throwable throwable, Object causedObject) {
		StackTrace stackTrace = new StackTrace(throwable.getStackTrace());

		fatalErrors.recordException(throwable, causedObject);

		ExceptionStats stats = allFatalErrors.get(stackTrace);
		if (stats == null) {
			stats = new ExceptionStats();
			allFatalErrors.put(stackTrace, stats);
		}
		stats.recordException(throwable, causedObject);
	}

	public void recordIoError(Throwable throwable, Object causedObject) {
		ioErrors.recordException(throwable, causedObject);
	}

	public void resetStats() {
		selectorSelectTime.resetStats();
		businessLogicTime.resetStats();

		selectedKeys.resetStats();
		invalidKeys.resetStats();
		acceptKeys.resetStats();
		connectKeys.resetStats();
		readKeys.resetStats();
		writeKeys.resetStats();

		localTasks.resetStats();
		concurrentTasks.resetStats();
		scheduledTasks.resetStats();

		localTaskDuration.resetStats();
		concurrentTaskDuration.resetStats();
		scheduledTaskDuration.resetStats();

		selectedKeysTime.resetStats();
		localTasksTime.resetStats();
		concurrentTasksTime.resetStats();
		scheduledTasksTime.resetStats();

		fatalErrors.resetStats();
		allFatalErrors.clear();
		ioErrors.resetStats();

		lastLongestLocalRunnable.reset();
		lastLongestConcurrentRunnable.reset();
		lastLongestScheduledRunnable.reset();
	}

	@JmxAttribute
	public ValueStats getSelectorSelectTime() {
		return selectorSelectTime;
	}

	@JmxAttribute
	public ValueStats getBusinessLogicTime() {
		return businessLogicTime;
	}

	@JmxAttribute
	public EventStats getSelectedKeys() {
		return selectedKeys;
	}

	@JmxAttribute
	public EventStats getInvalidKeys() {
		return invalidKeys;
	}

	@JmxAttribute
	public EventStats getAcceptKeys() {
		return acceptKeys;
	}

	@JmxAttribute
	public EventStats getConnectKeys() {
		return connectKeys;
	}

	@JmxAttribute
	public EventStats getReadKeys() {
		return readKeys;
	}

	@JmxAttribute
	public EventStats getWriteKeys() {
		return writeKeys;
	}

	@JmxAttribute
	public EventStats getLocalTasks() {
		return localTasks;
	}

	@JmxAttribute
	public EventStats getConcurrentTasks() {
		return concurrentTasks;
	}

	@JmxAttribute
	public EventStats getScheduledTasks() {
		return scheduledTasks;
	}

	@JmxAttribute
	public ValueStats getLocalTaskDuration() {
		return localTaskDuration;
	}

	public DurationRunnable getLastLongestLocalRunnable() {
		return lastLongestLocalRunnable;
	}

	@JmxAttribute
	public ValueStats getConcurrentTaskDuration() {
		return concurrentTaskDuration;
	}

	public DurationRunnable getLastLongestConcurrentRunnable() {
		return lastLongestConcurrentRunnable;
	}

	@JmxAttribute
	public ValueStats getScheduledTaskDuration() {
		return scheduledTaskDuration;
	}

	public DurationRunnable getLastLongestScheduledRunnable() {
		return lastLongestScheduledRunnable;
	}

	@JmxAttribute
	public ValueStats getSelectedKeysTime() {
		return selectedKeysTime;
	}

	@JmxAttribute
	public ValueStats getLocalTasksTime() {
		return localTasksTime;
	}

	@JmxAttribute
	public ValueStats getConcurrentTasksTime() {
		return concurrentTasksTime;
	}

	@JmxAttribute
	public ValueStats getScheduledTasksTime() {
		return scheduledTasksTime;
	}

	@JmxAttribute
	public ExceptionStats getIoErrors() {
		return ioErrors;
	}

	@JmxAttribute
	public List<ExceptionStats> getAllFatalErrors() {
		return new ArrayList<>(allFatalErrors.values());
	}

	@JmxAttribute
	public ExceptionStats getFatalErrors() {
		return fatalErrors;
	}

	@JmxAttribute(name = "")
	public BusinessLogicTimeHistogram getBusinessLogicTimeHistogram() {
		return businessLogicTimeHistogram;
	}

	public static final class BusinessLogicTimeHistogram
			implements JmxStats<BusinessLogicTimeHistogram> {
		// BLT - business logic time
		private static final int[] BLT_HISTOGRAM_BUCKETS = new int[]{0, 1, 10, 100, 1000, 10_000, 100_000};
		private static final List<String> BLT_HISTOGRAM_LABELS =
				asList(
						"0 - 1       ->  ",
						"1 - 10      ->  ",
						"10 - 100    ->  ",
						"100 - 1k    ->  ",
						"1k - 10k    ->  ",
						"10k - 100k  ->  ",
						"100k+       ->  "
				);

		private final long[] businessLogicTimeHistogram = new long[7];

		@JmxAttribute
		public List<String> getBusinessLogicTimeHistogram() {
			List<String> lines = new ArrayList<>(businessLogicTimeHistogram.length);
			for (int i = 0; i < businessLogicTimeHistogram.length; i++) {
				lines.add(BLT_HISTOGRAM_LABELS.get(i) + businessLogicTimeHistogram[i]);
			}
			return lines;
		}

		public void update(long businessLogicTime) {
			for (int i = 1; i < BLT_HISTOGRAM_BUCKETS.length; i++) {
				if (businessLogicTime <= BLT_HISTOGRAM_BUCKETS[i]) {
					int bucketIndex = i - 1;
					businessLogicTimeHistogram[bucketIndex]++;
					return;
				}
			}
			int lastBucket = BLT_HISTOGRAM_BUCKETS.length - 1;
			businessLogicTimeHistogram[lastBucket]++;
		}

		@Override
		public void add(BusinessLogicTimeHistogram another) {
			for (int i = 0; i < businessLogicTimeHistogram.length; i++) {
				businessLogicTimeHistogram[i] += another.businessLogicTimeHistogram[i];
			}
		}
	}
}
