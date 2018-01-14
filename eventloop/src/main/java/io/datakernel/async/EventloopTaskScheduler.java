package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.StageStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;

public final class EventloopTaskScheduler implements EventloopService, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final AsyncCallable<?> task;
	private final StageStats stats;

	private long initialDelay;
	private long period;
	private long interval;
	private double backoffFactor = 1.0;
	private double backoffFactorMax = 1.0;
	private boolean abortOnError = false;

	private long lastStartTime;
	private long lastCompleteTime;
	private Object lastResult;
	private Throwable lastError;
	private int lastErrorsCount;

	public interface ScheduleFunction<T> {
		long nextTimestamp(long now, long lastStartTime, long lastCompleteTime, T lastResult, Throwable lastError, int lastErrorsCount);
	}

	private ScheduleFunction<Object> scheduleFunction = (now, lastStartTime, lastCompleteTime, lastResult, lastError, lastErrorsCount) -> {
		long from;
		long delay;
		if (period != 0) {
			from = lastStartTime;
			delay = period;
		} else {
			from = lastCompleteTime;
			delay = interval;
		}
		if (lastErrorsCount != 0 && backoffFactor != 1.0) {
			double backoff = Math.pow(backoffFactor, lastErrorsCount);
			if (backoff > backoffFactorMax) {
				backoff = backoffFactorMax;
			}
			delay *= backoff;
		}
		long timestamp = from + delay;
		if (timestamp < now) {
			timestamp = now;
		}
		return timestamp;
	};

	private ScheduledRunnable scheduledTask;

	private EventloopTaskScheduler(Eventloop eventloop, AsyncCallable<?> task, StageStats stats) {
		this.eventloop = eventloop;
		this.task = task;
		this.stats = stats;
	}

	public static EventloopTaskScheduler create(Eventloop eventloop, AsyncCallable<?> task) {
		StageStats stats = StageStats.create(SMOOTHING_WINDOW_5_MINUTES);
		return new EventloopTaskScheduler(eventloop, task, stats);
	}

	public EventloopTaskScheduler withInitialDelay(long initialDelayMillis) {
		this.initialDelay = initialDelayMillis;
		return this;
	}

	public EventloopTaskScheduler withPeriod(long refreshPeriodMillis) {
		this.period = refreshPeriodMillis;
		return this;
	}

	public EventloopTaskScheduler withInterval(long intervalMillis) {
		this.interval = intervalMillis;
		return this;
	}

	@SuppressWarnings("unchecked")
	public EventloopTaskScheduler withScheduleFunction(ScheduleFunction<?> scheduleFunction) {
		this.scheduleFunction = (ScheduleFunction<Object>) scheduleFunction;
		return this;
	}

	public EventloopTaskScheduler withAbortOnError(boolean abortOnError) {
		this.abortOnError = abortOnError;
		return this;
	}

	public EventloopTaskScheduler withStatsSmoothingWindow(double smoothingWindowSeconds) {
		this.stats.setSmoothingWindow(smoothingWindowSeconds);
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

		long timestamp = (lastStartTime == 0) ?
				eventloop.currentTimeMillis() + initialDelay :
				scheduleFunction.nextTimestamp(eventloop.currentTimeMillis(), lastStartTime, lastCompleteTime, lastResult, lastError, lastErrorsCount);

		scheduledTask = eventloop.scheduleBackground(
				timestamp,
				() -> {
					long startTime = eventloop.currentTimeMillis();
					stats.monitor(task).whenComplete((result, throwable) -> {
						if (throwable == null) {
							lastStartTime = startTime;
							lastCompleteTime = eventloop.currentTimeMillis();
							lastResult = result;
							lastError = null;
							lastErrorsCount = 0;
							scheduleTask();
						} else {
							lastStartTime = startTime;
							lastCompleteTime = eventloop.currentTimeMillis();
							lastResult = null;
							lastError = throwable;
							lastErrorsCount++;
							logger.error("Task failures: " + lastErrorsCount, throwable);
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

	@JmxAttribute
	public long getPeriod() {
		return period;
	}

	@JmxAttribute
	public void setPeriod(long refreshPeriodMillis) {
		this.period = refreshPeriodMillis;
	}

	@JmxAttribute
	public long getInterval() {
		return interval;
	}

	@JmxAttribute
	public void setInterval(long interval) {
		this.interval = interval;
	}

	@JmxAttribute
	public double getBackoffFactor() {
		return backoffFactor;
	}

	@JmxAttribute
	public void setBackoffFactor(double backoffFactor) {
		this.backoffFactor = backoffFactor;
	}

	@JmxAttribute
	public double getBackoffFactorMax() {
		return backoffFactorMax;
	}

	@JmxAttribute
	public void setBackoffFactorMax(double backoffFactorMax) {
		this.backoffFactorMax = backoffFactorMax;
	}

	@JmxAttribute
	public StageStats getStats() {
		return stats;
	}

	@JmxAttribute
	public double getStatsSmoothingWindow() {
		return stats.getSmoothingWindow();
	}

	@JmxAttribute
	public void setStatsSmoothingWindow(double smoothingWindowSeconds) {
		stats.setSmoothingWindow(smoothingWindowSeconds);
	}

	@JmxOperation
	public void resetStats() {
		stats.resetStats();
	}

}
