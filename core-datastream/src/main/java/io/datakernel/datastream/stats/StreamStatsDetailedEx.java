package io.datakernel.datastream.stats;

import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxReducers.JmxReducerSum;
import io.datakernel.jmx.stats.EventStats;
import io.datakernel.jmx.stats.ValueStats;
import org.jetbrains.annotations.Nullable;

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
	public StreamStatsDetailedEx<T> withBasicSmoothingWindow(Duration smoothingWindow) {
		return (StreamStatsDetailedEx<T>) super.withBasicSmoothingWindow(smoothingWindow);
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

	public StreamStatsDetailedEx<T> withSizeHistogram(int[] levels) {
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
	@JmxAttribute(reducer = JmxReducerSum.class)
	public Long getTotalSizeAvg() {
		return sizeCounter != null && getStarted().getTotalCount() != 0 ?
				totalSize.getTotalCount() / getStarted().getTotalCount() :
				null;
	}

}
