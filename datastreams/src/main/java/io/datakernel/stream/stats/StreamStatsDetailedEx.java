package io.datakernel.stream.stats;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.jmx.ValueStats;
import io.datakernel.stream.StreamDataAcceptor;

import java.time.Duration;

public final class StreamStatsDetailedEx<T> extends StreamStatsBasic<T> {
	public static final Duration DEFAULT_DETAILED_SMOOTHING_WINDOW = Duration.ofMinutes(1);

	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private final EventStats count = EventStats.create(DEFAULT_DETAILED_SMOOTHING_WINDOW).withRateUnit("data items");
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
	public StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor) {
		return sizeCounter == null ?
				new StreamDataAcceptor<T>() {
					final EventStats count = StreamStatsDetailedEx.this.count;

					@Override
					public void accept(T item) {
						count.recordEvent();
						actualDataAcceptor.accept(item);
					}
				} :
				new StreamDataAcceptor<T>() {
					final EventStats count = StreamStatsDetailedEx.this.count;
					final ValueStats itemSize = StreamStatsDetailedEx.this.itemSize;

					@Override
					public void accept(T item) {
						count.recordEvent();
						int size = sizeCounter.size(item);
						itemSize.recordValue(size);
						totalSize.recordEvents(size);
						actualDataAcceptor.accept(item);
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

	@Nullable
	@JmxAttribute
	public ValueStats getItemSize() {
		return sizeCounter != null ? itemSize : null;
	}

	@Nullable
	@JmxAttribute
	public EventStats getTotalSize() {
		return sizeCounter != null ? totalSize : null;
	}

	@Nullable
	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public Long getTotalSizeAvg() {
		return sizeCounter != null && super.getStarted().getTotalCount() != 0 ?
				totalSize.getTotalCount() / super.getStarted().getTotalCount() :
				null;
	}

}
