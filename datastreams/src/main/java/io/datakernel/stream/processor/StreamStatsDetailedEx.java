package io.datakernel.stream.processor;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.*;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_1_MINUTE;

public final class StreamStatsDetailedEx extends StreamStatsBasic implements StreamStats.Receiver<Object> {
	public static final double DEFAULT_DETAILED_SMOOTHING_WINDOW = SMOOTHING_WINDOW_1_MINUTE;

	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private final EventStats count = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);
	private final ValueStats sizeStats = ValueStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);
	private long totalSize;

	@SuppressWarnings("unchecked")
	private StreamStatsDetailedEx(StreamStatsSizeCounter<?> sizeCounter) {
		this.sizeCounter = (StreamStatsSizeCounter<Object>) sizeCounter;
	}

	public static StreamStatsDetailedEx create() {
		return new StreamStatsDetailedEx(null);
	}

	public static StreamStatsDetailedEx create(StreamStatsSizeCounter<?> sizeCounter) {
		return new StreamStatsDetailedEx(sizeCounter);
	}

	@SuppressWarnings("unchecked")
	@Override
	public StreamStatsDetailedEx withBasicSmoothingWindow(double smoothingWindowSeconds) {
		return (StreamStatsDetailedEx) super.withBasicSmoothingWindow(smoothingWindowSeconds);
	}

	public StreamStatsDetailedEx withDetailedSmoothingWindow(double smoothingWindowSeconds) {
		count.setSmoothingWindow(smoothingWindowSeconds);
		sizeStats.setSmoothingWindow(smoothingWindowSeconds);
		return this;
	}

	public StreamStatsDetailedEx withSizeHistogram(int[] levels) {
		sizeStats.setHistogramLevels(levels);
		return this;
	}

	@Override
	public void onData(Object item) {
		count.recordEvent();
		if (sizeCounter != null) {
			int size = sizeCounter.size(item);
			totalSize += size;
			sizeStats.recordValue(size);
		}
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public EventStats getCount() {
		return count;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public long getTotalSize() {
		return totalSize;
	}

	@JmxAttribute
	public ValueStats getSizeStats() {
		return sizeStats;
	}

	@Override
	@JmxOperation
	public void resetStats() {
		super.resetStats();
		count.resetStats();
		totalSize = 0;
		sizeStats.resetStats();
	}
}
