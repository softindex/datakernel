package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import io.datakernel.util.Initializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public final class EventloopTaskScheduler implements EventloopService, Initializable<EventloopTaskScheduler>, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final AsyncSupplier<?> task;
	private final StageStats stats = StageStats.create(Duration.ofMinutes(5));

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

	public interface Schedule {
		long nextTimestamp(long now, long lastStartTime, long lastCompleteTime);

		/**
		 * Scheduling immediate execution
		 */
		static Schedule immediate() {
			return (now, lastStartTime, lastCompleteTime) -> now;
		}

		/**
		 * Scheduling task after delay
		 */
		static Schedule ofDelay(Duration delay) {
			return ofDelay(delay.toMillis());
		}

		/**
		 * @see Schedule#ofDelay(Duration)
		 */
		static Schedule ofDelay(long delay) {
			return (now, lastStartTime, lastCompleteTime) -> now + delay;
		}

		/**
		 * Scheduling task after last complete time and next task
		 */
		static Schedule ofInterval(Duration interval) {
			return ofInterval(interval.toMillis());
		}

		/**
		 * @see Schedule#ofInterval(Duration)
		 */
		static Schedule ofInterval(long interval) {
			return (now, lastStartTime, lastCompleteTime) -> lastCompleteTime + interval;
		}

		/**
		 * Scheduling task in period of current and next task
		 */
		static Schedule ofPeriod(Duration period) {
			return ofPeriod(period.toMillis());
		}

		/**
		 * @see Schedule#ofPeriod(Duration)
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

	private Stage<Void> doCall() {
		lastStartTime = eventloop.currentTimeMillis();
		return task.get()
				.whenComplete(stats.recordStats())
				.whenComplete((result, throwable) -> {
					lastCompleteTime = eventloop.currentTimeMillis();
					if (throwable == null) {
						firstRetryTime = 0;
						lastException = null;
						errorCount = 0;
						scheduleTask();
					} else {
						lastException = throwable;
						errorCount++;
						logger.error("Retry attempt " + errorCount, throwable);
						if (abortOnError) {
							if (scheduledTask != null) {
								scheduledTask.cancel();
							}
							throw new RuntimeException(throwable);
						} else {
							scheduleTask();
						}
					}
				})
				.toVoid();
	}

	@Override
	public Stage<Void> start() {
		scheduleTask();
		return Stage.of(null);
	}

	@Override
	public Stage<Void> stop() {
		if (scheduledTask != null) {
			scheduledTask.cancel();
		}
		return Stage.of(null);
	}

	public void setSchedule(Schedule schedule) {
		this.schedule = schedule;
		if (stats.getActiveStages() != 0 && scheduledTask != null && !scheduledTask.isCancelled()) {
			scheduledTask.cancel();
			scheduledTask = null;
			scheduleTask();
		}
	}

	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		if (stats.getActiveStages() != 0 && scheduledTask != null && !scheduledTask.isCancelled() && lastException != null) {
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
		if (stats.getActiveStages() == 0) {
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
	public StageStats getStats() {
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
