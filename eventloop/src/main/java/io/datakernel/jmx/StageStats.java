package io.datakernel.jmx;

import io.datakernel.async.AsyncCallable;
import io.datakernel.eventloop.Eventloop;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.jmx.JmxReducers.JmxReducerMax;
import static io.datakernel.jmx.JmxReducers.JmxReducerSum;
import static io.datakernel.jmx.MBeanFormat.formatPeriodAgo;

public class StageStats implements EventloopJmxMBean {
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

	public static StageStats createMBean(Eventloop eventloop, double smoothingWindowSeconds) {
		return new StageStats(eventloop, ValueStats.create(smoothingWindowSeconds));
	}

	public static StageStats create(double smoothingWindowSeconds) {
		return new StageStats(null, ValueStats.create(smoothingWindowSeconds));
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

	public <T> CompletionStage<T> monitor(CompletionStage<T> stage) {
		return stage.whenComplete(recordStats());
	}

	public <T> BiConsumer<T, Throwable> recordStats() {
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

	@Override
	public Eventloop getEventloop() {
		return eventloop;
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
		return lastStartTimestamp != 0 ? formatPeriodAgo(lastStartTimestamp) : "";
	}

	@JmxAttribute(reducer = JmxReducerMax.class, optional = true)
	public long getLastCompleteTimestamp() {
		return lastCompleteTimestamp;
	}

	@JmxAttribute
	public String getLastCompleteTime() {
		return lastCompleteTimestamp != 0 ? formatPeriodAgo(lastCompleteTimestamp) : "";
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

	@JmxAttribute(optional = true)
	public double getSmoothingWindow() {
		return duration.getSmoothingWindow();
	}

	@JmxAttribute(optional = true)
	public void setSmoothingWindow(double smoothingWindowSeconds) {
		duration.setSmoothingWindow(smoothingWindowSeconds);
	}

	@JmxOperation
	public void resetStats() {
		duration.resetStats();
		exceptions.resetStats();
	}
}