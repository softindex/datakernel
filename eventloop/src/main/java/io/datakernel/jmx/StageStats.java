package io.datakernel.jmx;

import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stage;
import io.datakernel.async.StageConsumer;
import io.datakernel.eventloop.Eventloop;

import java.time.Duration;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.jmx.JmxReducers.JmxReducerMax;
import static io.datakernel.jmx.JmxReducers.JmxReducerSum;

public class StageStats {
	private Eventloop eventloop;

	private int activeStages = 0;
	private long lastStartTimestamp = 0;
	private long lastCompleteTimestamp = 0;
	private final ValueStats duration;
	private final ExceptionStats exceptions = ExceptionStats.create();

	StageStats(Eventloop eventloop, ValueStats duration) {
		this.eventloop = eventloop;
		this.duration = duration;
	}

	public static StageStats createMBean(Eventloop eventloop, Duration smoothingWindowSeconds) {
		return new StageStats(eventloop, ValueStats.create(smoothingWindowSeconds));
	}

//	public static StageStats create(double smoothingWindowSeconds) {
//		return new StageStats(null, ValueStats.create(smoothingWindowSeconds));
//	}

	public static StageStats create(Duration smoothingWindow) {
		return new StageStats(null, ValueStats.create(smoothingWindow));
	}

	public StageStats withHistogram(int[] levels) {
		setHistogramLevels(levels);
		return this;
	}

	public void setHistogramLevels(int[] levels) {
		duration.setHistogramLevels(levels);
	}

	private long currentTimeMillis() {
		if (eventloop == null) {
			eventloop = getCurrentEventloop();
		}
		return eventloop.currentTimeMillis();
	}

	public <T> AsyncCallable<T> wrapper(AsyncCallable<T> callable) {
		return () -> monitor(callable.call());
	}

	public <T> Stage<T> monitor(Stage<T> stage) {
		return stage.whenComplete(recordStats());
	}

	public <T> StageConsumer<T> recordStats() {
		this.activeStages++;
		long before = currentTimeMillis();
		this.lastStartTimestamp = before;
		return (value, throwable) -> {
			this.activeStages--;
			long now = currentTimeMillis();
			long durationMillis = now - before;
			this.lastCompleteTimestamp = now;
			duration.recordValue(durationMillis);

			if (throwable != null) {
				exceptions.recordException(throwable);
			}
		};
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getActiveStages() {
		return activeStages;
	}

	@JmxAttribute(reducer = JmxReducerMax.class, optional = true)
	public long getLastStartTimestamp() {
		return lastStartTimestamp;
	}

	@JmxAttribute
	public String getLastStartTime() {
		return lastStartTimestamp != 0 ? MBeanFormat.formatTimestamp(lastStartTimestamp) : "";
	}

	@JmxAttribute(reducer = JmxReducerMax.class, optional = true)
	public long getLastCompleteTimestamp() {
		return lastCompleteTimestamp;
	}

	@JmxAttribute
	public String getLastCompleteTime() {
		return lastCompleteTimestamp != 0 ? MBeanFormat.formatTimestamp(lastCompleteTimestamp) : "";
	}

	@JmxAttribute(reducer = JmxReducerMax.class)
	public long getCurrentDuration() {
		return activeStages != 0 ? currentTimeMillis() - lastStartTimestamp : 0;
	}

	@JmxAttribute
	public ValueStats getDuration() {
		return duration;
	}

	@JmxAttribute
	public ExceptionStats getExceptions() {
		return exceptions;
	}
}