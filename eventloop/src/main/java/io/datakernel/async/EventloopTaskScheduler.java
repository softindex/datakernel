package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.util.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;

public final class EventloopTaskScheduler implements EventloopService, Initializer<EventloopTaskScheduler>, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final AsyncCallable<?> task;
	private final StageStats stats = StageStats.create(SMOOTHING_WINDOW_5_MINUTES);

	private long initialDelay;
	private Schedule schedule;
	private RetryPolicy retryPolicy;

	private boolean abortOnError = false;

	private long lastStartTime;
	private long lastCompleteTime;
	private Throwable lastError;
	private long firstRetryTime;
	private int errorCount;

	private Long period;
	private Long interval;

	public interface Schedule {
		long nextTimestamp(long now, long lastStartTime, long lastCompleteTime);

		static Schedule immediate() {
			return (now, lastStartTime, lastCompleteTime) -> now;
		}

		static Schedule ofDelay(long delay) {
			return (now, lastStartTime, lastCompleteTime) -> now + delay;
		}

		static Schedule ofInterval(long interval) {
			return (now, lastStartTime, lastCompleteTime) -> lastCompleteTime + interval;
		}

		static Schedule ofPeriod(long period) {
			return (now, lastStartTime, lastCompleteTime) -> lastStartTime + period;
		}
	}

	private ScheduledRunnable scheduledTask;

	private EventloopTaskScheduler(Eventloop eventloop, AsyncCallable<?> task) {
		this.eventloop = eventloop;
		this.task = task;
	}

	public static EventloopTaskScheduler create(Eventloop eventloop, AsyncCallable<?> task) {
		return new EventloopTaskScheduler(eventloop, task);
	}

	public EventloopTaskScheduler withInitialDelay(long initialDelayMillis) {
		this.initialDelay = initialDelayMillis;
		return this;
	}

	public EventloopTaskScheduler withSchedule(Schedule schedule) {
		this.schedule = schedule;
		// for JMX:
		this.period = null;
		this.interval = null;
		return this;
	}

	public EventloopTaskScheduler withPeriod(long periodMillis) {
		setPeriod(periodMillis);
		return this;
	}

	public EventloopTaskScheduler withInterval(long intervalMillis) {
		setInterval(intervalMillis);
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
		if (scheduledTask != null && scheduledTask.isCancelled())
			return;

		long now = eventloop.currentTimeMillis();
		long timestamp;
		if (lastStartTime == 0) {
			timestamp = now + initialDelay;
		} else if (lastError == null || retryPolicy == null) {
			timestamp = schedule.nextTimestamp(now, lastStartTime, lastCompleteTime);
		} else {
			assert errorCount != 0;
			if (firstRetryTime == 0) firstRetryTime = now;
			timestamp = retryPolicy.nextRetryTimestamp(now, lastError, errorCount - 1, firstRetryTime);
			if (timestamp == 0) {
				timestamp = schedule.nextTimestamp(now, lastStartTime, lastCompleteTime);
			}
		}

		scheduledTask = eventloop.scheduleBackground(timestamp,
				() -> {
					lastStartTime = eventloop.currentTimeMillis();
					task.call()
							.whenComplete(stats.recordStats())
							.whenComplete((result, throwable) -> {
								lastCompleteTime = eventloop.currentTimeMillis();
								if (throwable == null) {
									firstRetryTime = 0;
									lastError = null;
									errorCount = 0;
									scheduleTask();
								} else {
									lastError = throwable;
									errorCount++;
									logger.error("Retry attempt " + errorCount, throwable);
									if (abortOnError) {
										scheduledTask.cancel();
										throw new RuntimeException(throwable);
									} else {
										scheduleTask();
									}
								}
							});
				});
	}

	@Override
	public CompletionStage<Void> start() {
		scheduleTask();
		return Stages.of(null);
	}

	@Override
	public CompletionStage<Void> stop() {
		scheduledTask.cancel();
		return Stages.of(null);
	}

	@JmxAttribute(name = "")
	public StageStats getStats() {
		return stats;
	}

	@JmxAttribute
	public long getInitialDelay() {
		return initialDelay;
	}

	@JmxAttribute
	public Long getPeriod() {
		return period;
	}

	@JmxAttribute
	public void setPeriod(Long periodMillis) {
		this.schedule = Schedule.ofPeriod(periodMillis);
		// for JMX:
		this.period = periodMillis;
		this.interval = null;
	}

	@JmxAttribute
	public Long getInterval() {
		return interval;
	}

	@JmxAttribute
	public void setInterval(Long intervalMillis) {
		this.schedule = Schedule.ofInterval(intervalMillis);
		// for JMX:
		this.period = null;
		this.interval = intervalMillis;
	}

}
