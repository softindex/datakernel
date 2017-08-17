package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.jmx.JmxAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.async.AsyncCallbacks.toResultCallback;

public final class EventloopTaskScheduler<T> implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final AsyncCallable<T> task;

	private long initialDelay;
	private long period;
	private long interval;
	private double backoffFactor = 1.0;
	private double backoffFactorMax = 1.0;
	private boolean abortOnError = false;

	private long lastStartTime;
	private long lastCompleteTime;
	private T lastResult;
	private Exception lastError;
	private int lastErrorsCount;

	// JMX

	private int totalCount;
	private long totalDuration;
	private int totalErrorsCount;
	private long totalErrorsDuration;

	public interface TimestampFunction<T> {
		long nextTimestamp(long now, long lastStartTime, long lastCompleteTime, T lastResult, Exception lastError, int lastErrorsCount);
	}

	private TimestampFunction<T> timestampFunction = (now, lastStartTime, lastCompleteTime, lastResult, lastError, lastErrorsCount) -> {
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

	private EventloopTaskScheduler(Eventloop eventloop, AsyncCallable<T> task) {
		this.eventloop = eventloop;
		this.task = task;
	}

	public static <T> EventloopTaskScheduler<T> create(Eventloop eventloop, AsyncCallable<T> task) {
		return new EventloopTaskScheduler<>(eventloop, task);
	}

	public static EventloopTaskScheduler<Void> create(Eventloop eventloop, AsyncRunnable task) {
		return new EventloopTaskScheduler<>(eventloop, callback -> task.run(toResultCallback(callback, null)));
	}

	public EventloopTaskScheduler<T> withInitialDelay(long initialDelayMillis) {
		this.initialDelay = initialDelayMillis;
		return this;
	}

	public EventloopTaskScheduler<T> withPeriod(long refreshPeriodMillis) {
		this.period = refreshPeriodMillis;
		return this;
	}

	public EventloopTaskScheduler<T> withInterval(long intervalMillis) {
		this.interval = intervalMillis;
		return this;
	}

	public EventloopTaskScheduler<T> withTimestampFunction(TimestampFunction<T> timestampFunction) {
		this.timestampFunction = timestampFunction;
		return this;
	}

	public EventloopTaskScheduler<T> withAbortOnError(boolean abortOnError) {
		this.abortOnError = abortOnError;
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
				timestampFunction.nextTimestamp(eventloop.currentTimeMillis(), lastStartTime, lastCompleteTime, lastResult, lastError, lastErrorsCount);

		scheduledTask = eventloop.scheduleBackground(
				timestamp,
				() -> {
					long startTime = eventloop.currentTimeMillis();
					task.call(new ResultCallback<T>() {
						@Override
						protected void onResult(T result) {
							lastStartTime = startTime;
							lastCompleteTime = eventloop.currentTimeMillis();
							lastResult = result;
							lastError = null;
							lastErrorsCount = 0;
							totalCount++;
							totalDuration += lastCompleteTime - lastStartTime;
							scheduleTask();
						}

						@Override
						protected void onException(Exception e) {
							lastStartTime = startTime;
							lastCompleteTime = eventloop.currentTimeMillis();
							lastResult = null;
							lastError = e;
							lastErrorsCount++;
							totalErrorsCount++;
							totalErrorsDuration += lastCompleteTime - lastStartTime;
							logger.error("Task failures: " + lastErrorsCount, e);
							if (abortOnError) {
								scheduledTask.cancel();
								throw new RuntimeException(e);
							} else {
								scheduleTask();
							}
						}
					});
				});
	}

	@Override
	public void start(CompletionCallback callback) {
		scheduleTask();
	}

	@Override
	public void stop(CompletionCallback callback) {
		scheduledTask.cancel();
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
	public long getLastStartTime() {
		return lastStartTime;
	}

	@JmxAttribute
	public long getLastCompleteTime() {
		return lastCompleteTime;
	}

	@JmxAttribute
	public T getLastResult() {
		return lastResult;
	}

	@JmxAttribute
	public Exception getLastError() {
		return lastError;
	}

	@JmxAttribute
	public int getLastErrorsCount() {
		return lastErrorsCount;
	}

	@JmxAttribute
	public int getTotalCount() {
		return totalCount;
	}

	@JmxAttribute
	public long getTotalDuration() {
		return totalDuration;
	}

	@JmxAttribute
	public double getAverageDuration() {
		return totalCount == 0 ? 0 : (totalDuration / totalCount);
	}

	@JmxAttribute
	public int getTotalErrorsCount() {
		return totalErrorsCount;
	}

	@JmxAttribute
	public long getTotalErrorsDuration() {
		return totalErrorsDuration;
	}

	@JmxAttribute
	public double getAverageErrorsDuration() {
		return totalErrorsCount == 0 ? 0 : (totalErrorsDuration / totalErrorsCount);
	}

}
