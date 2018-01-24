package io.datakernel.stream.stats;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.jmx.ValueStats;
import io.datakernel.stream.StreamDataReceiver;

import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_1_MINUTE;

public final class StreamStatsDetailedEx extends StreamStatsBasic {
	public static final double DEFAULT_DETAILED_SMOOTHING_WINDOW = SMOOTHING_WINDOW_1_MINUTE;

	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private final EventStats count = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);
	private final ValueStats itemSize = ValueStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);
	private final EventStats totalSize = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);

	@SuppressWarnings("unchecked")
	StreamStatsDetailedEx(StreamStatsSizeCounter<?> sizeCounter) {
		this.sizeCounter = (StreamStatsSizeCounter<Object>) sizeCounter;
	}

	@SuppressWarnings("unchecked")
	@Override
	public StreamStatsDetailedEx withBasicSmoothingWindow(double smoothingWindowSeconds) {
		return (StreamStatsDetailedEx) super.withBasicSmoothingWindow(smoothingWindowSeconds);
	}

	@Override
	public <T> StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver) {
		return sizeCounter == null ?
				new StreamDataReceiver<T>() {
					final EventStats count = StreamStatsDetailedEx.this.count;

					@Override
					public void onData(T item) {
						count.recordEvent();
						actualDataReceiver.onData(item);
					}
				} :
				new StreamDataReceiver<T>() {
					final EventStats count = StreamStatsDetailedEx.this.count;
					final ValueStats itemSize = StreamStatsDetailedEx.this.itemSize;

					@Override
					public void onData(T item) {
						count.recordEvent();
						int size = sizeCounter.size(item);
						itemSize.recordValue(size);
						totalSize.recordEvents(size);
						actualDataReceiver.onData(item);
					}
				};
	}

	public StreamStatsDetailedEx withSizeHistogram(int[] levels) {
		itemSize.setHistogramLevels(levels);
		return this;
	}

	@JmxAttribute
	public EventStats getCount() {
		return count;
	}

	@JmxAttribute
	public ValueStats getItemSize() {
		return sizeCounter != null ? itemSize : null;
	}

	@JmxAttribute
	public EventStats getTotalSize() {
		return sizeCounter != null ? totalSize : null;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public Long getTotalSizeAvg() {
		return sizeCounter != null && super.getStarted().getTotalCount() != 0 ?
				totalSize.getTotalCount() / super.getStarted().getTotalCount() :
				null;
	}

}
