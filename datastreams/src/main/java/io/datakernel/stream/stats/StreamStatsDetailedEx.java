package io.datakernel.stream.stats;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.jmx.ValueStats;
import io.datakernel.stream.StreamDataReceiver;

import java.time.Duration;

public final class StreamStatsDetailedEx<T> extends StreamStatsBasic<T> {
	public static final Duration DEFAULT_DETAILED_SMOOTHING_WINDOW = Duration.ofMinutes(1);

	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private final EventStats count = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);
	private final ValueStats itemSize = ValueStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);
	private final EventStats totalSize = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW);

	@SuppressWarnings("unchecked")
	StreamStatsDetailedEx(StreamStatsSizeCounter<?> sizeCounter) {
		this.sizeCounter = (StreamStatsSizeCounter<Object>) sizeCounter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public StreamStatsDetailedEx withBasicSmoothingWindow(Duration smoothingWindow) {
		return (StreamStatsDetailedEx) super.withBasicSmoothingWindow(smoothingWindow);
	}

	@Override
	public StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver) {
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
