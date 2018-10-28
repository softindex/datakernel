package io.datakernel.jmx;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;

import java.time.Duration;
import java.time.Instant;
import java.util.function.BiConsumer;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.jmx.JmxReducers.JmxReducerSum;

public class PromiseStats {
	private Eventloop eventloop;

	private int activePromises = 0;
	private long lastStartTimestamp = 0;
	private long lastCompleteTimestamp = 0;
	private final ValueStats duration;
	private final ExceptionStats exceptions = ExceptionStats.create();

	protected PromiseStats(Eventloop eventloop, ValueStats duration) {
		this.eventloop = eventloop;
		this.duration = duration;
	}

	public static PromiseStats createMBean(Eventloop eventloop, Duration smoothingWindow) {
		return new PromiseStats(eventloop, ValueStats.create(smoothingWindow));
	}

	public static PromiseStats create(Duration smoothingWindow) {
		return new PromiseStats(null, ValueStats.create(smoothingWindow));
	}

	public PromiseStats withHistogram(int[] levels) {
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

	public <T> AsyncSupplier<T> wrapper(AsyncSupplier<T> callable) {
		return () -> monitor(callable.get());
	}

	public <T> Promise<T> monitor(Promise<T> promise) {
		return promise.whenComplete(recordStats());
	}

	public <T> BiConsumer<T, Throwable> recordStats() {
		this.activePromises++;
		long before = currentTimeMillis();
		this.lastStartTimestamp = before;
		return (value, throwable) -> {
			this.activePromises--;
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
	public long getActivePromises() {
		return activePromises;
	}

	@JmxAttribute
	@Nullable
	public Instant getLastStartTime() {
		return lastStartTimestamp != 0L ? Instant.ofEpochMilli(lastStartTimestamp) : null;
	}

	@JmxAttribute
	@Nullable
	public Instant getLastCompleteTime() {
		return lastCompleteTimestamp != 0L ? Instant.ofEpochMilli(lastCompleteTimestamp) : null;
	}

	@JmxAttribute
	@Nullable
	public Duration getCurrentDuration() {
		return activePromises != 0 ? Duration.ofMillis(currentTimeMillis() - lastStartTimestamp) : null;
	}

	@JmxAttribute
	public ValueStats getDuration() {
		return duration;
	}

	@JmxAttribute
	public ExceptionStats getExceptions() {
		return exceptions;
	}

	@Override
	public String toString() {
		return "PromiseStats{" +
				"activePromises=" + activePromises +
				", lastStartTimestamp=" + MBeanFormat.formatTimestamp(lastStartTimestamp) +
				", lastCompleteTimestamp=" + MBeanFormat.formatTimestamp(lastCompleteTimestamp) +
				", duration=" + duration +
				", exceptions=" + exceptions +
				'}';
	}
}
