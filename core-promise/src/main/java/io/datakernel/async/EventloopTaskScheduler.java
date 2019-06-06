/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.util.Initializable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("UnusedReturnValue")
public final class EventloopTaskScheduler implements EventloopService, Initializable<EventloopTaskScheduler>, EventloopJmxMBeanEx {
	private static final Logger logger = Logger.getLogger(EventloopTaskScheduler.class.getName());

	private final Eventloop eventloop;
	private final AsyncSupplier<?> task;
	private final PromiseStats stats = PromiseStats.create(Duration.ofMinutes(5));

	private long initialDelay;
	private Schedule schedule;
	private RetryPolicy retryPolicy;

	private boolean abortOnError = false;

	private long lastStartTime;
	private long lastCompleteTime;
	@Nullable
	private Throwable lastException;
	private long firstRetryTime;
	private int errorCount;

	@Nullable
	private Duration period;
	@Nullable
	private Duration interval;
	private boolean enabled = true;

	@FunctionalInterface
	public interface Schedule {
		long nextTimestamp(long now, long lastStartTime, long lastCompleteTime);

		/**
		 * Schedules immediate execution.
		 */
		static Schedule immediate() {
			return (now, lastStartTime, lastCompleteTime) -> now;
		}

		/**
		 * Schedules task after delay.
		 */
		static Schedule ofDelay(Duration delay) {
			return ofDelay(delay.toMillis());
		}

		/**
		 * @see #ofDelay(Duration)
		 */
		static Schedule ofDelay(long delay) {
			return (now, lastStartTime, lastCompleteTime) -> now + delay;
		}

		/**
		 * @see #ofInterval(long)
		 */
		static Schedule ofInterval(Duration interval) {
			return ofInterval(interval.toMillis());
		}

		/**
		 * Schedules task after last complete time and next task.
		 */
		static Schedule ofInterval(long interval) {
			return (now, lastStartTime, lastCompleteTime) -> lastCompleteTime + interval;
		}

		/**
		 * @see #ofPeriod(long)
		 */
		static Schedule ofPeriod(Duration period) {
			return ofPeriod(period.toMillis());
		}


		/**
		 * Schedules task in period of current and next task.
		 */
		static Schedule ofPeriod(long period) {
			return (now, lastStartTime, lastCompleteTime) -> lastStartTime + period;
		}
	}

	@Nullable
	private ScheduledRunnable scheduledTask;

	private EventloopTaskScheduler(Eventloop eventloop, AsyncSupplier<?> task) {
		this.eventloop = eventloop;
		this.task = task;
	}

	public static <T> EventloopTaskScheduler create(Eventloop eventloop, AsyncSupplier<T> task) {
		return new EventloopTaskScheduler(eventloop, task);
	}

	public EventloopTaskScheduler withInitialDelay(Duration initialDelay) {
		this.initialDelay = initialDelay.toMillis();
		return this;
	}

	public EventloopTaskScheduler withSchedule(Schedule schedule) {
		this.schedule = schedule;
		// for JMX:
		this.period = null;
		this.interval = null;
		return this;
	}

	public EventloopTaskScheduler withPeriod(Duration period) {
		setPeriod(period);
		return this;
	}

	public EventloopTaskScheduler withInterval(Duration interval) {
		setInterval(interval);
		return this;
	}

	public EventloopTaskScheduler withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	public EventloopTaskScheduler withAbortOnError(boolean abortOnError) {
		this.abortOnError = abortOnError;
		return this;
	}

	public EventloopTaskScheduler withStatsHistogramLevels(int[] levels) {
		this.stats.setHistogramLevels(levels);
		return this;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private void scheduleTask() {
		if (schedule == null || scheduledTask != null && scheduledTask.isCancelled())
			return;

		if (!enabled) return;

		long now = eventloop.currentTimeMillis();
		long timestamp;
		if (lastStartTime == 0) {
			timestamp = now + initialDelay;
		} else if (lastException == null || retryPolicy == null) {
			timestamp = schedule.nextTimestamp(now, lastStartTime, lastCompleteTime);
		} else {
			assert errorCount != 0;
			if (firstRetryTime == 0) firstRetryTime = now;
			timestamp = retryPolicy.nextRetryTimestamp(now, lastException, errorCount - 1, firstRetryTime);
			if (timestamp == 0) {
				timestamp = schedule.nextTimestamp(now, lastStartTime, lastCompleteTime);
			}
		}

		scheduledTask = eventloop.scheduleBackground(timestamp, doCall::get);
	}

	private final AsyncSupplier<Void> doCall = AsyncSuppliers.reuse(this::doCall);

	private Promise<Void> doCall() {
		lastStartTime = eventloop.currentTimeMillis();
		return task.get()
				.whenComplete(stats.recordStats())
				.whenComplete((result, e) -> {
					lastCompleteTime = eventloop.currentTimeMillis();
					if (e == null) {
						firstRetryTime = 0;
						lastException = null;
						errorCount = 0;
						scheduleTask();
					} else {
						lastException = e;
						errorCount++;
						logger.log(Level.SEVERE, "Retry attempt " + errorCount, e);
						if (abortOnError) {
							if (scheduledTask != null) {
								scheduledTask.cancel();
							}
							throw new RuntimeException(e);
						} else {
							scheduleTask();
						}
					}
				})
				.toVoid();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		scheduleTask();
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		if (scheduledTask != null) {
			scheduledTask.cancel();
		}
		return Promise.complete();
	}

	public void setSchedule(Schedule schedule) {
		this.schedule = schedule;
		if (stats.getActivePromises() != 0 && scheduledTask != null && !scheduledTask.isCancelled()) {
			scheduledTask.cancel();
			scheduledTask = null;
			scheduleTask();
		}
	}

	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		if (stats.getActivePromises() != 0 && scheduledTask != null && !scheduledTask.isCancelled() && lastException != null) {
			scheduledTask.cancel();
			scheduledTask = null;
			scheduleTask();
		}
	}

	@JmxAttribute
	public boolean isEnabled() {
		return enabled;
	}

	@JmxAttribute
	public void setEnabled(boolean enabled) {
		if (this.enabled == enabled) return;
		this.enabled = enabled;
		if (stats.getActivePromises() == 0) {
			if (enabled) {
				scheduleTask();
			} else {
				if (scheduledTask != null && !scheduledTask.isCancelled()) {
					scheduledTask.cancel();
					scheduledTask = null;
				}
			}
		}
	}

	@JmxAttribute(name = "")
	public PromiseStats getStats() {
		return stats;
	}

	@JmxAttribute
	@Nullable
	public Throwable getLastException() {
		return lastException;
	}

	@JmxAttribute
	public long getInitialDelay() {
		return initialDelay;
	}

	@JmxAttribute
	@Nullable
	public Duration getPeriod() {
		return period;
	}

	@JmxAttribute
	public void setPeriod(Duration period) {
		Schedule schedule = Schedule.ofPeriod(period);
		setSchedule(schedule);
		// for JMX:
		this.period = period;
		this.interval = null;
	}

	@JmxAttribute
	@Nullable
	public Duration getInterval() {
		return interval;
	}

	@JmxAttribute
	public void setInterval(Duration interval) {
		setSchedule(Schedule.ofInterval(interval));
		// for JMX:
		this.period = null;
		this.interval = interval;
	}

	@JmxOperation
	public void startNow() {
		doCall.get();
	}

}
