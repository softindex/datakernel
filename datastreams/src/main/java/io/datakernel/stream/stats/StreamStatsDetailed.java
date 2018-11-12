package io.datakernel.stream.stats;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.jmx.JmxStatsWithReset;
import io.datakernel.stream.StreamDataAcceptor;
import io.datakernel.util.ReflectionUtils;

import java.time.Duration;

public final class StreamStatsDetailed<T> extends StreamStatsBasic<T> implements JmxStatsWithReset {
	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private long count;
	private long totalSize;

	@SuppressWarnings("unchecked")
	StreamStatsDetailed(@Nullable StreamStatsSizeCounter<?> sizeCounter) {
		this.sizeCounter = (StreamStatsSizeCounter<Object>) sizeCounter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public StreamStatsDetailed<T> withBasicSmoothingWindow(Duration smoothingWindow) {
		return (StreamStatsDetailed<T>) super.withBasicSmoothingWindow(smoothingWindow);
	}

	@Override
	public StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor) {
		return sizeCounter == null ?
				item -> {
					count++;
					actualDataAcceptor.accept(item);
				} :
				item -> {
					count++;
					int size = sizeCounter.size(item);
					totalSize += size;
					actualDataAcceptor.accept(item);
				};
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public long getCount() {
		return count;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	@Nullable
	public Long getTotalSize() {
		return sizeCounter != null ? totalSize : null;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	@Nullable
	public Long getTotalSizeAvg() {
		return sizeCounter != null && getStarted().getTotalCount() != 0 ?
				totalSize / getStarted().getTotalCount() :
				null;
	}

	@Override
	public void resetStats() {
		count = totalSize = 0;
		ReflectionUtils.resetStats(this);
	}
}
