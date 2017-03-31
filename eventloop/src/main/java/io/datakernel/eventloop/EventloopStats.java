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
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.util.Stopwatch;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.datakernel.jmx.ValueStats.POWERS_OF_TWO;

@SuppressWarnings("unused")
public final class EventloopStats {
	private final EventStats loops;
	private final ValueStats selectorSelectTimeout;
	private final ValueStats selectorSelectTime;
	private final ValueStats businessLogicTime;
	private final Tasks tasks;
	private final Keys keys;
	private final ErrorStats errorStats;
	private final EventStats selectorSpinnings;

	private EventloopStats(double smoothingWindow, Eventloop.ExtraStatsExtractor extraStatsExtractor) {
		loops = EventStats.create(smoothingWindow);
		selectorSelectTimeout = ValueStats.create(smoothingWindow).withHistogram(new int[]{
				-256, -128, -64, -32, -16, -8, -4, -2, -1, 0, 1, 2, 4, 8, 16, 32});
		selectorSelectTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
		businessLogicTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
		tasks = new Tasks(smoothingWindow, extraStatsExtractor);
		keys = new Keys(smoothingWindow);
		errorStats = new ErrorStats();
		selectorSpinnings = EventStats.create(smoothingWindow);
	}

	public static EventloopStats create(double smoothingWindow, Eventloop.ExtraStatsExtractor extraStatsExtractor) {
		return new EventloopStats(smoothingWindow, extraStatsExtractor);
	}

	public void setSmoothingWindow(double smoothingWindow) {
		loops.setSmoothingWindow(smoothingWindow);
		selectorSelectTimeout.setSmoothingWindow(smoothingWindow);
		selectorSelectTime.setSmoothingWindow(smoothingWindow);
		businessLogicTime.setSmoothingWindow(smoothingWindow);
		tasks.setSmoothingWindow(smoothingWindow);
		keys.setSmoothingWindow(smoothingWindow);
	}

	public void reset() {
		loops.resetStats();
		selectorSelectTimeout.resetStats();
		selectorSelectTime.resetStats();
		businessLogicTime.resetStats();
		tasks.reset();
		keys.reset();
		errorStats.reset();
		selectorSpinnings.resetStats();
	}

	// region updating
	public void updateBusinessLogicTime(long businessLogicTime) {
		loops.recordEvent();
		this.businessLogicTime.recordValue((int) businessLogicTime);
	}

	public void updateSelectorSelectTime(long selectorSelectTime) {
		this.selectorSelectTime.recordValue((int) selectorSelectTime);
	}

	public void updateSelectorSelectTimeout(long selectorSelectTimeout) {
		this.selectorSelectTimeout.recordValue((int) selectorSelectTimeout);
	}

	public void updateSelectedKeyDuration(Stopwatch sw) {
		if (sw != null) {
			keys.oneKeyTime.recordValue((int) sw.elapsed(TimeUnit.MICROSECONDS));
		}
	}

	public void updateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys,
	                                    int connectKeys, int readKeys, int writeKeys, long loopTime) {
		keys.all.recordEvents(lastSelectedKeys);
		keys.invalid.recordEvents(invalidKeys);
		keys.accept.recordValue(acceptKeys);
		keys.connect.recordValue(connectKeys);
		keys.read.recordValue(readKeys);
		keys.write.recordValue(writeKeys);
		keys.loopTime.recordValue((int) loopTime);
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
		updateTaskDuration(tasks.local.oneTaskTime, tasks.local.longestTask, runnable, sw);
	}

	public void updateLocalTasksStats(int newTasks, long loopTime) {
		tasks.local.loopTime.recordValue((int) loopTime);
		tasks.local.tasksPerLoop.recordValue(newTasks);
	}

	public void updateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(tasks.concurrent.oneTaskTime, tasks.concurrent.longestTask, runnable, sw);
	}

	public void updateConcurrentTasksStats(int newTasks, long loopTime) {
		tasks.concurrent.loopTime.recordValue((int) loopTime);
		tasks.concurrent.tasksPerLoop.recordValue(newTasks);
	}

	public void updateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw, boolean background) {
		if (background) {
			updateTaskDuration(tasks.background.getOneTaskTime(), tasks.background.getLongestTask(), runnable, sw);
		} else {
			updateTaskDuration(tasks.scheduled.getOneTaskTime(), tasks.scheduled.getLongestTask(), runnable, sw);
		}
	}

	public void updateScheduledTasksStats(int newTasks, long loopTime, boolean background) {
		if (background) {
			tasks.background.getLoopTime().recordValue((int) loopTime);
			tasks.background.getTasksPerLoop().recordValue(newTasks);
		} else {
			tasks.scheduled.getLoopTime().recordValue((int) loopTime);
			tasks.scheduled.getTasksPerLoop().recordValue(newTasks);
		}
	}

	public void recordFatalError(Throwable throwable, Object causedObject) {
		StackTrace stackTrace = new StackTrace(throwable.getStackTrace());

		errorStats.fatalErrors.recordException(throwable, causedObject);

		ExceptionStats stats = errorStats.allFatalErrors.get(stackTrace);
		if (stats == null) {
			stats = ExceptionStats.create().withStoreStackTrace(true);
			errorStats.allFatalErrors.put(stackTrace, stats);
		}
		stats.recordException(throwable, causedObject);
	}

	public void recordIoError(Throwable throwable, Object causedObject) {
		errorStats.ioErrors.recordException(throwable, causedObject);
	}

	public void recordScheduledTaskOverdue(int overdue, boolean background) {
		if (background) {
			tasks.background.overdues.recordValue(overdue);
		} else {
			tasks.scheduled.overdues.recordValue(overdue);
		}
	}

	public void updateProcessedTasksAndKeys(int tasksAndKeys) {
		if (tasksAndKeys == 0) selectorSpinnings.recordEvent();
	}
	// endregion

	// region root attributes
	@JmxAttribute
	public EventStats getLoops() {
		return loops;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public ValueStats getSelectorSelectTime() {
		return selectorSelectTime;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public ValueStats getSelectorSelectTimeout() {
		return selectorSelectTimeout;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public ValueStats getBusinessLogicTime() {
		return businessLogicTime;
	}

	@JmxAttribute
	public Tasks getTasks() {
		return tasks;
	}

	@JmxAttribute
	public Keys getKeys() {
		return keys;
	}

	@JmxAttribute
	public ErrorStats getErrorStats() {
		return errorStats;
	}

	@JmxAttribute
	public EventStats getSelectorSpinnings() {
		return selectorSpinnings;
	}

	// endregion

	// region helper classes for stats grouping
	public static final class Tasks {
		private final TaskStats local;
		private final TaskStats concurrent;
		private final ScheduledTaskStats scheduled;
		private final ScheduledTaskStats background;

		public Tasks(double smoothingWindow, final Eventloop.ExtraStatsExtractor extraStatsExtractor) {
			local = new TaskStats(smoothingWindow, new Count() {
				@Override
				public int getCount() {
					return extraStatsExtractor.getLocalTasksCount();
				}
			});

			concurrent = new TaskStats(smoothingWindow, new Count() {
				@Override
				public int getCount() {
					return extraStatsExtractor.getConcurrentTasksCount();
				}
			});

			scheduled = new ScheduledTaskStats(smoothingWindow, new Count() {
				@Override
				public int getCount() {
					return extraStatsExtractor.getScheduledTasksCount();
				}
			});

			background = new ScheduledTaskStats(smoothingWindow, new Count() {
				@Override
				public int getCount() {
					return extraStatsExtractor.getBackgroundTasksCount();
				}
			});
		}

		public void setSmoothingWindow(double smoothingWindow) {
			local.setSmoothingWindow(smoothingWindow);
			concurrent.setSmoothingWindow(smoothingWindow);
			scheduled.setSmoothingWindow(smoothingWindow);
			background.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			local.reset();
			concurrent.reset();
			scheduled.reset();
			background.reset();
		}

		@JmxAttribute
		public TaskStats getLocal() {
			return local;
		}

		@JmxAttribute
		public TaskStats getConcurrent() {
			return concurrent;
		}

		@JmxAttribute
		public ScheduledTaskStats getScheduled() {
			return scheduled;
		}

		@JmxAttribute
		public ScheduledTaskStats getBackground() {
			return background;
		}
	}

	public static class TaskStats {
		private final ValueStats tasksPerLoop;
		private final ValueStats loopTime;
		private final ValueStats oneTaskTime;
		private final DurationRunnable longestTask;
		private final Count count;

		public TaskStats(double smoothingWindow, Count count) {
			this.tasksPerLoop = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			this.loopTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			this.oneTaskTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			this.longestTask = new DurationRunnable();
			this.count = count;
		}

		public void setSmoothingWindow(double smoothingWindow) {
			tasksPerLoop.setSmoothingWindow(smoothingWindow);
			loopTime.setSmoothingWindow(smoothingWindow);
			oneTaskTime.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			tasksPerLoop.resetStats();
			loopTime.resetStats();
			oneTaskTime.resetStats();
		}

		@JmxAttribute(name = "perLoop", extraSubAttributes = "histogram")
		public ValueStats getTasksPerLoop() {
			return tasksPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getLoopTime() {
			return loopTime;
		}

		@JmxAttribute(name = "oneTaskTime(μs)", extraSubAttributes = "histogram")
		public ValueStats getOneTaskTime() {
			return oneTaskTime;
		}

		@JmxAttribute
		public DurationRunnable getLongestTask() {
			return longestTask;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public int getCount() {
			return count.getCount();
		}
	}

	public static final class ScheduledTaskStats extends TaskStats {
		private final ValueStats overdues;

		public ScheduledTaskStats(double smoothingWindow, Count count) {
			super(smoothingWindow, count);
			overdues = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
		}

		public void setSmoothingWindow(double smoothingWindow) {
			super.setSmoothingWindow(smoothingWindow);
			overdues.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			super.reset();
			overdues.resetStats();
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getOverdues() {
			return overdues;
		}
	}

	public static final class Keys {
		private final EventStats all;
		private final EventStats invalid;
		private final ValueStats accept;
		private final ValueStats connect;
		private final ValueStats read;
		private final ValueStats write;
		private final ValueStats loopTime;
		private final ValueStats oneKeyTime;

		public Keys(double smoothingWindow) {
			all = EventStats.create(smoothingWindow);
			invalid = EventStats.create(smoothingWindow);
			accept = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			connect = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			read = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			write = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			loopTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
			oneKeyTime = ValueStats.create(smoothingWindow).withHistogram(POWERS_OF_TWO);
		}

		public void setSmoothingWindow(double smoothingWindow) {
			all.setSmoothingWindow(smoothingWindow);
			invalid.setSmoothingWindow(smoothingWindow);
			accept.setSmoothingWindow(smoothingWindow);
			connect.setSmoothingWindow(smoothingWindow);
			read.setSmoothingWindow(smoothingWindow);
			write.setSmoothingWindow(smoothingWindow);
			loopTime.setSmoothingWindow(smoothingWindow);
			oneKeyTime.setSmoothingWindow(smoothingWindow);
		}

		public void reset() {
			all.resetStats();
			invalid.resetStats();
			accept.resetStats();
			connect.resetStats();
			read.resetStats();
			write.resetStats();
			loopTime.resetStats();
			oneKeyTime.resetStats();
		}

		@JmxAttribute
		public EventStats getAll() {
			return all;
		}

		@JmxAttribute
		public EventStats getInvalid() {
			return invalid;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getAccept() {
			return accept;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getConnect() {
			return connect;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getRead() {
			return read;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getWrite() {
			return write;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getLoopTime() {
			return loopTime;
		}

		@JmxAttribute(name = "oneKeyTime(μs)", extraSubAttributes = "histogram")
		public ValueStats getOneKeyTime() {
			return oneKeyTime;
		}
	}

	private interface Count {
		int getCount();
	}

	public static final class ErrorStats {
		private final ExceptionStats fatalErrors = ExceptionStats.create().withStoreStackTrace(true);
		private final Map<StackTrace, ExceptionStats> allFatalErrors = new HashMap<>();
		private final ExceptionStats ioErrors = ExceptionStats.create();

		public void reset() {
			fatalErrors.resetStats();
			allFatalErrors.clear();
			ioErrors.resetStats();
		}

		@JmxAttribute
		public ExceptionStats getFatalErrors() {
			return fatalErrors;
		}

		@JmxAttribute
		public Map<StackTrace, ExceptionStats> getAllFatalErrors() {
			return allFatalErrors;
		}

		@JmxAttribute
		public ExceptionStats getIoErrors() {
			return ioErrors;
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

	public static final class DurationRunnable implements JmxStats<DurationRunnable> {
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

		@JmxAttribute(name = "duration(μs)")
		public long getDuration() {
			return duration;
		}

		@JmxAttribute
		public String getClassName() {
			return (runnable == null) ? "" : runnable.getClass().getName();
		}

		@Override
		public void add(DurationRunnable another) {
			if (another.duration > this.duration) {
				this.duration = another.duration;
				this.runnable = another.runnable;
			}
		}
	}
	// endregion
}
