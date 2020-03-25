package io.datakernel.datastream.stats;

import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.jmx.ExceptionStats;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxReducers.JmxReducerSum;

import java.time.Duration;

public class StreamStatsBasic<T> implements StreamStats<T> {
	public static final Duration DEFAULT_BASIC_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final EventStats started = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats resume = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats suspend = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final EventStats endOfStream = EventStats.create(DEFAULT_BASIC_SMOOTHING_WINDOW);
	private final ExceptionStats error = ExceptionStats.create();

	public StreamStatsBasic<T> withBasicSmoothingWindow(Duration smoothingWindow) {
		started.setSmoothingWindow(smoothingWindow);
		resume.setSmoothingWindow(smoothingWindow);
		suspend.setSmoothingWindow(smoothingWindow);
		endOfStream.setSmoothingWindow(smoothingWindow);
		return this;
	}

	@Override
	public StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor) {
		return actualDataAcceptor;
	}

	@Override
	public void onStarted() {
		started.recordEvent();
	}

	@Override
	public void onResume() {
		resume.recordEvent();
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
	public void onError(Throwable e) {
		error.recordException(e);
	}

	@JmxAttribute
	public EventStats getStarted() {
		return started;
	}

	@JmxAttribute
	public EventStats getResume() {
		return resume;
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

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActive() {
		return (int) (started.getTotalCount() - (endOfStream.getTotalCount() + error.getTotal()));
	}

}
