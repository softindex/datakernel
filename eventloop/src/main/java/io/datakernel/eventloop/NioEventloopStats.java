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

import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.*;
import io.datakernel.util.ExceptionMarker;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class NioEventloopStats implements NioEventloopStatsMBean {
	private static final long DEFAULT_LONGLOOP_TIME = 500; // 500 ms

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

	private final DynamicStatsCounter selectorSelectTimeStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter businessLogicTimeStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter selectedKeysStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter invalidKeysStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter acceptKeysStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter connectKeysStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter readKeysStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter writeKeysStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter localTasksStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter concurrentTasksStats = new DynamicStatsCounter(1 << 16);
	private final DynamicStatsCounter scheduledTasksStats = new DynamicStatsCounter(1 << 16);

	private final StatsCounter localTaskDuration = new StatsCounter();
	private final DurationRunnable lastLongestLocalRunnable = new DurationRunnable();
	private final StatsCounter concurrentTaskDuration = new StatsCounter();
	private final DurationRunnable lastLongestConcurrentRunnable = new DurationRunnable();
	private final StatsCounter scheduledTaskDuration = new StatsCounter();
	private final DurationRunnable lastLongestScheduledRunnable = new DurationRunnable();

	private final StatsCounter selectedKeysTimeStats = new StatsCounter();
	private final StatsCounter localTasksTimeStats = new StatsCounter();
	private final StatsCounter concurrentTasksTimeStats = new StatsCounter();
	private final StatsCounter scheduledTasksTimeStats = new StatsCounter();

	private boolean monitoring;
	private long monitoringTimestamp;
	private long monitoringLoop;

	// long loop monitoring
	private volatile long longLoopMillis = DEFAULT_LONGLOOP_TIME;
	private final RateCounter longLoopsRate = new RateCounter();
	private final StatsCounter longLoopLocalTasksStats = new StatsCounter();
	private final StatsCounter longLoopConcurrentTasksStats = new StatsCounter();
	private final StatsCounter longLoopScheduledTasksStats = new StatsCounter();
	private String longLoopLongestLocalTask;
	private String longLoopLongestConcurrentTask;
	private String longLoopLongestScheduledTask;
	private StatsSnapshot lastLongLoopStats;

	void incMonitoringLoop() {
		if (isMonitoring()) {
			monitoringLoop++;
		}
	}

	void updateBusinessLogicTime(long timestamp, long businessLogicTime) {
		businessLogicTimeStats.add((int) businessLogicTime);

		if (!isMonitoring())
			return;
		if (businessLogicTime > longLoopMillis) {
			longLoopsRate.incNumerator();
			longLoopLocalTasksStats.add(localTasksStats.getLastValue());
			longLoopConcurrentTasksStats.add(concurrentTasksStats.getLastValue());
			longLoopScheduledTasksStats.add(scheduledTasksStats.getLastValue());
			longLoopLongestLocalTask = lastLongestLocalRunnable.toString();
			longLoopLongestConcurrentTask = lastLongestConcurrentRunnable.toString();
			longLoopLongestScheduledTask = lastLongestScheduledRunnable.toString();
			lastLongLoopStats = getStatsSnapshot(timestamp);
		}
		longLoopsRate.incDenominator();
		if (longLoopsRate.getDenominator() < 0) {
			longLoopsRate.reset();
		}
	}

	void updateSelectorSelectTime(long selectorSelectTime) {
		selectorSelectTimeStats.add((int) selectorSelectTime);
	}

	void updateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys) {
		selectedKeysStats.add(lastSelectedKeys);
		invalidKeysStats.add(invalidKeys);
		acceptKeysStats.add(acceptKeys);
		connectKeysStats.add(connectKeys);
		readKeysStats.add(readKeys);
		writeKeysStats.add(writeKeys);
	}

	void updateSelectedKeysTimeStats(@Nullable Stopwatch sw) {
		if (sw != null)
			selectedKeysTimeStats.add((int) sw.elapsed(TimeUnit.MILLISECONDS));
	}

	private void updateTaskDuration(StatsCounter counter, DurationRunnable longestCounter, Runnable runnable, @Nullable Stopwatch sw) {
		if (sw != null) {
			int elapsed = (int) sw.elapsed(TimeUnit.MICROSECONDS);
			counter.add(elapsed);
			if (elapsed > longestCounter.getDuration()) {
				longestCounter.update(runnable, elapsed);
			}
		}
	}

	void updateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(localTaskDuration, lastLongestLocalRunnable, runnable, sw);
	}

	void updateLocalTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			localTasksTimeStats.add((int) sw.elapsed(TimeUnit.MILLISECONDS));
		localTasksStats.add(newTasks);
	}

	void updateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(concurrentTaskDuration, lastLongestConcurrentRunnable, runnable, sw);
	}

	void updateConcurrentTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			concurrentTasksTimeStats.add((int) sw.elapsed(TimeUnit.MICROSECONDS));
		concurrentTasksStats.add(newTasks);
	}

	void updateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(scheduledTaskDuration, lastLongestScheduledRunnable, runnable, sw);
	}

	void updateScheduledTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			scheduledTasksTimeStats.add((int) sw.elapsed(TimeUnit.MILLISECONDS));
		scheduledTasksStats.add(newTasks);
	}

	// Exceptions stats
	private static final class ExceptionMarkerImpl implements ExceptionMarker {
		private final Class<?> clazz;
		private final Marker marker;

		ExceptionMarkerImpl(Class<?> clazz, String name) {
			this.clazz = clazz;
			this.marker = MarkerFactory.getMarker(name);
		}

		@Override
		public Marker getMarker() {
			return marker;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ExceptionMarkerImpl that = (ExceptionMarkerImpl) o;
			return Objects.equal(this.clazz, that.clazz) &&
					Objects.equal(this.marker, that.marker);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(clazz, marker);
		}

		@Override
		public String toString() {
			return clazz.getName() + "." + marker.getName();
		}
	}

	private final Map<ExceptionMarker, LastExceptionCounter> exceptionCounters = new HashMap<>();

	public static ExceptionMarker exceptionMarker(Class<?> clazz, String name) {
		return new ExceptionMarkerImpl(clazz, name);
	}

	LastExceptionCounter getExceptionCounter(ExceptionMarker marker) {
		return exceptionCounters.get(marker);
	}

	LastExceptionCounter ensureExceptionCounter(ExceptionMarker marker) {
		if (!exceptionCounters.containsKey(marker))
			exceptionCounters.put(marker, new LastExceptionCounter(marker.getMarker()));
		return exceptionCounters.get(marker);
	}

	void updateExceptionCounter(ExceptionMarker marker, Throwable e, Object o, long timestamp) {
		ensureExceptionCounter(marker).update(e, o, timestamp);
	}

	void resetExceptionCounter(ExceptionMarker marker) {
		LastExceptionCounter counter = exceptionCounters.get(marker);
		if (counter != null)
			counter.reset();
	}

	// Snapshot stats
	public final class StatsSnapshot {
		private final long timestamp;
		private final long numberLoop;
		private final long selectorSelectTime;
		private final long selectedKeysTime;
		private final long acceptKeys;
		private final long connectKeys;
		private final long readKeys;
		private final long writeKeys;
		private final long invalidKeys;
		private final long localRunnables;
		private final long concurrentRunnables;
		private final long scheduledRunnables;
		private final long localRunnablesTime;
		private final long concurrentRunnablesTime;
		private final long scheduledRunnablesTime;

		private StatsSnapshot(long timestamp) {
			this.timestamp = timestamp;
			this.numberLoop = monitoringLoop;
			this.selectorSelectTime = selectorSelectTimeStats.getLastValue();
			this.selectedKeysTime = selectedKeysTimeStats.getLast();
			this.acceptKeys = acceptKeysStats.getLastValue();
			this.connectKeys = connectKeysStats.getLastValue();
			this.readKeys = readKeysStats.getLastValue();
			this.writeKeys = writeKeysStats.getLastValue();
			this.invalidKeys = invalidKeysStats.getLastValue();
			this.localRunnables = localTasksStats.getLastValue();
			this.concurrentRunnables = concurrentTasksStats.getLastValue();
			this.scheduledRunnables = scheduledTasksStats.getLastValue();
			this.localRunnablesTime = localTasksTimeStats.getLast();
			this.concurrentRunnablesTime = concurrentTasksTimeStats.getLast();
			this.scheduledRunnablesTime = scheduledTasksTimeStats.getLast();
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getNumberLoop() {
			return numberLoop;
		}

		public long getSelectorSelectTime() {
			return selectorSelectTime;
		}

		public long getSelectedKeysTime() {
			return selectedKeysTime;
		}

		public long getAcceptKeys() {
			return acceptKeys;
		}

		public long getConnectKeys() {
			return connectKeys;
		}

		public long getReadKeys() {
			return readKeys;
		}

		public long getWriteKeys() {
			return writeKeys;
		}

		public long getInvalidKeys() {
			return invalidKeys;
		}

		public long getLocalRunnables() {
			return localRunnables;
		}

		public long getConcurrentRunnables() {
			return concurrentRunnables;
		}

		public long getScheduledRunnables() {
			return scheduledRunnables;
		}

		public long getLocalRunnablesTime() {
			return localRunnablesTime;
		}

		public long getConcurrentRunnablesTime() {
			return concurrentRunnablesTime;
		}

		public long getScheduledRunnablesTime() {
			return scheduledRunnablesTime;
		}
	}

	public StatsSnapshot getStatsSnapshot(long timestamp) {
		if (!monitoring) return null;
		return new StatsSnapshot(timestamp);
	}

	// JMX
	@Override
	public void resetStats() {
		selectorSelectTimeStats.reset();
		businessLogicTimeStats.reset();

		selectedKeysStats.reset();
		invalidKeysStats.reset();
		acceptKeysStats.reset();
		connectKeysStats.reset();
		readKeysStats.reset();
		writeKeysStats.reset();

		localTasksStats.reset();
		concurrentTasksStats.reset();
		scheduledTasksStats.reset();

		localTaskDuration.reset();
		concurrentTaskDuration.reset();
		scheduledTaskDuration.reset();

		selectedKeysTimeStats.reset();
		localTasksTimeStats.reset();
		concurrentTasksTimeStats.reset();
		scheduledTasksTimeStats.reset();

		for (LastExceptionCounter counter : exceptionCounters.values()) {
			counter.reset();
		}

		longLoopsRate.reset();
		longLoopLocalTasksStats.reset();
		lastLongestLocalRunnable.reset();
		longLoopConcurrentTasksStats.reset();
		lastLongestConcurrentRunnable.reset();
		longLoopScheduledTasksStats.reset();
		lastLongestScheduledRunnable.reset();
	}

	@Override
	public void startMonitoring() {
		monitoring = true;
		monitoringTimestamp = System.currentTimeMillis();
		monitoringLoop = 0;

		lastLongestLocalRunnable.reset();
		lastLongestConcurrentRunnable.reset();
		lastLongestScheduledRunnable.reset();
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		monitoringTimestamp = 0;
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public long getMonitoringLoop() {
		return monitoringLoop;
	}

	@Override
	public String getMonitoringTime() {
		if (!isMonitoring())
			return null;
		return MBeanFormat.formatDuration(System.currentTimeMillis() - monitoringTimestamp);
	}

	@Override
	public int getSelectedKeys() {
		return selectedKeysStats.getLastValue();
	}

	@Override
	public long getInvalidKeys() {
		return invalidKeysStats.getLastValue();
	}

	@Override
	public long getAcceptKeys() {
		return acceptKeysStats.getLastValue();
	}

	@Override
	public long getConnectKeys() {
		return connectKeysStats.getLastValue();
	}

	@Override
	public long getReadKeys() {
		return readKeysStats.getLastValue();
	}

	@Override
	public long getWriteKeys() {
		return writeKeysStats.getLastValue();
	}

	@Override
	public String getSelectedKeysStats() {
		return selectedKeysStats.toString();
	}

	@Override
	public String getInvalidKeysStats() {
		return invalidKeysStats.toString();
	}

	@Override
	public String getAcceptKeysStats() {
		return acceptKeysStats.toString();
	}

	@Override
	public String getConnectKeysStats() {
		return connectKeysStats.toString();
	}

	@Override
	public String getReadKeysStats() {
		return readKeysStats.toString();
	}

	@Override
	public String getWriteKeysStats() {
		return writeKeysStats.toString();
	}

	@Override
	public int getSelectedKeysMillis() {
		return selectedKeysTimeStats.getLast();
	}

	@Override
	public String getSelectedKeysMillisStats() {
		return selectedKeysTimeStats.toString();
	}

	@Override
	public long getBusinessLogicMillis() {
		return businessLogicTimeStats.getLastValue();
	}

	@Override
	public String getBusinessLogicMillisStats() {
		return businessLogicTimeStats.toString();
	}

	@Override
	public long getSelectorSelectMillis() {
		return selectorSelectTimeStats.getLastValue();
	}

	@Override
	public String getSelectorSelectMillisStats() {
		return selectorSelectTimeStats.toString();
	}

	@Override
	public CompositeData[] getLastExceptions() throws OpenDataException {
		if (exceptionCounters.isEmpty())
			return null;
		List<CompositeData> results = new ArrayList<>();
		for (LastExceptionCounter counter : exceptionCounters.values()) {
			results.add(counter.compositeData());
		}
		return results.toArray(new CompositeData[results.size()]);
	}

	// Tasks stats
	@Override
	public int getLocalTaskMicros() {
		return localTaskDuration.getLast();
	}

	@Override
	public String getLocalTaskStats() {
		return localTaskDuration.toString();
	}

	@Override
	public String getLocalTaskLongestMicros() {
		return lastLongestLocalRunnable.toString();
	}

	@Override
	public int getLocalTasksMillis() {
		return localTasksTimeStats.getLast();
	}

	@Override
	public String getLocalTasksStats() {
		return localTasksTimeStats.toString();
	}

	@Override
	public int getLocalTasksPerLoop() {
		return localTasksStats.getLastValue();
	}

	@Override
	public String getLocalTasksPerLoopStats() {
		return localTasksStats.toString();
	}

	@Override
	public int getConcurrentTaskMicros() {
		return concurrentTaskDuration.getLast();
	}

	@Override
	public String getConcurrentTaskStats() {
		return concurrentTaskDuration.toString();
	}

	@Override
	public String getConcurrentTaskLongestMicros() {
		return lastLongestConcurrentRunnable.toString();
	}

	@Override
	public int getConcurrentTasksMillis() {
		return concurrentTasksTimeStats.getLast();
	}

	@Override
	public String getConcurrentTasksStats() {
		return concurrentTasksTimeStats.toString();
	}

	@Override
	public int getConcurrentTasksPerLoop() {
		return concurrentTasksStats.getLastValue();
	}

	@Override
	public String getConcurrentTasksPerLoopStats() {
		return concurrentTasksStats.toString();
	}

	@Override
	public int getScheduledTaskMicros() {
		return scheduledTaskDuration.getLast();
	}

	@Override
	public String getScheduledTaskStats() {
		return scheduledTaskDuration.toString();
	}

	@Override
	public String getScheduledTaskLongestMicros() {
		return lastLongestScheduledRunnable.toString();
	}

	@Override
	public int getScheduledTasksMillis() {
		return scheduledTasksTimeStats.getLast();
	}

	@Override
	public String getScheduledTasksStats() {
		return scheduledTasksTimeStats.toString();
	}

	@Override
	public int getScheduledTasksPerLoop() {
		return scheduledTasksStats.getLastValue();
	}

	@Override
	public String getScheduledTasksPerLoopStats() {
		return scheduledTasksStats.toString();
	}

	// long loop stats
	@Override
	public long getLongLoopMillis() {
		return longLoopMillis;
	}

	@Override
	public void setLongLoopMillis(long timeLongLoop) {
		this.longLoopMillis = timeLongLoop;
	}

	@Override
	public String getLongLoopsRate() {
		return longLoopsRate.toString();
	}

	@Override
	public long getLongLoopLocalTasks() {
		return longLoopLocalTasksStats.getLast();
	}

	@Override
	public String getLongLoopLocalTasksStats() {
		return longLoopLocalTasksStats.toString();
	}

	@Override
	public String getLongLoopLocalTaskLongest() {
		return longLoopLongestLocalTask;
	}

	@Override
	public long getLongLoopConcurrentTasks() {
		return longLoopConcurrentTasksStats.getLast();
	}

	@Override
	public String getLongLoopConcurrentTasksStats() {
		return longLoopConcurrentTasksStats.toString();
	}

	@Override
	public String getLongLoopConcurrentTaskLongest() {
		return longLoopLongestConcurrentTask;
	}

	@Override
	public long getLongLoopScheduledTasks() {
		return longLoopScheduledTasksStats.getLast();
	}

	@Override
	public String getLongLoopScheduledTasksStats() {
		return longLoopScheduledTasksStats.toString();
	}

	@Override
	public String getLongLoopScheduledTaskLongest() {
		return longLoopLongestScheduledTask;
	}

	@Override
	public CompositeData getLastLongLoopStats() throws OpenDataException {
		if (lastLongLoopStats == null)
			return null;
		return CompositeDataBuilder.builder("LongLoop stats")
				.add("NumberLoop", SimpleType.LONG, lastLongLoopStats.getNumberLoop())
				.add("SelectorSelectMillis", SimpleType.LONG, lastLongLoopStats.getSelectorSelectTime())
				.add("SelectedKeysMillis", SimpleType.LONG, lastLongLoopStats.getSelectedKeysTime())
				.add("AcceptKeys", SimpleType.LONG, lastLongLoopStats.getAcceptKeys())
				.add("ConnectKeys", SimpleType.LONG, lastLongLoopStats.getConnectKeys())
				.add("ReadKeys", SimpleType.LONG, lastLongLoopStats.getReadKeys())
				.add("WriteKeys", SimpleType.LONG, lastLongLoopStats.getWriteKeys())
				.add("InvalidKeys", SimpleType.LONG, lastLongLoopStats.getInvalidKeys())
				.add("LocalRunnables", SimpleType.LONG, lastLongLoopStats.getLocalRunnables())
				.add("ConcurrentRunnables", SimpleType.LONG, lastLongLoopStats.getConcurrentRunnables())
				.add("ScheduledRunnables", SimpleType.LONG, lastLongLoopStats.getScheduledRunnables())
				.add("LocalRunnablesMillis", SimpleType.LONG, lastLongLoopStats.getLocalRunnablesTime())
				.add("ConcurrentRunnablesMillis", SimpleType.LONG, lastLongLoopStats.getConcurrentRunnablesTime())
				.add("ScheduledRunnablesMillis", SimpleType.LONG, lastLongLoopStats.getScheduledRunnablesTime())
				.build();
	}

}
