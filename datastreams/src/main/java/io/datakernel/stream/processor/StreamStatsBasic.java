package io.datakernel.stream.processor;

import io.datakernel.jmx.*;
import io.datakernel.stream.StreamDataReceiver;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;

public class StreamStatsBasic implements StreamStats {
	public static final double DEFAULT_BASIC_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final EventStats started = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats produce = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats suspend = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats endOfStream = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final ExceptionStats error = ExceptionStats.create();

	public StreamStatsBasic withBasicSmoothingWindow(double smoothingWindowSeconds) {
		started.setSmoothingWindow(smoothingWindowSeconds);
		produce.setSmoothingWindow(smoothingWindowSeconds);
		suspend.setSmoothingWindow(smoothingWindowSeconds);
		endOfStream.setSmoothingWindow(smoothingWindowSeconds);
		return this;
	}

	@Override
	public <T> StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver) {
		return actualDataReceiver;
	}

	@Override
	public void onStarted() {
		started.recordEvent();
	}

	@Override
	public void onProduce() {
		produce.recordEvent();
	}

	@Override
	public void onSuspend() {
		suspend.recordEvent();
	}

	@Override
	public void onEndOfStream() {
		endOfStream.recordEvent();
	}

	@Override
	public void onError(Throwable throwable) {
		error.recordException(throwable);
	}

	@JmxAttribute
	public EventStats getStarted() {
		return started;
	}

	@JmxAttribute
	public EventStats getProduce() {
		return produce;
	}

	@JmxAttribute
	public EventStats getSuspend() {
		return suspend;
	}

	@JmxAttribute
	public EventStats getEndOfStream() {
		return endOfStream;
	}

	@JmxAttribute
	public ExceptionStats getError() {
		return error;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getActive() {
		return (int) (started.getTotalCount() - (endOfStream.getTotalCount() + error.getTotal()));
	}

	@JmxOperation
	public void resetStats() {
		started.resetStats();
		produce.resetStats();
		suspend.resetStats();
		endOfStream.resetStats();
		error.resetStats();
	}
}