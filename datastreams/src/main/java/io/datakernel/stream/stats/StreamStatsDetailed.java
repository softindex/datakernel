package io.datakernel.stream.stats;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.jmx.JmxStatsWithReset;
import io.datakernel.jmx.ReflectionUtils;
import io.datakernel.stream.StreamDataReceiver;

public final class StreamStatsDetailed extends StreamStatsBasic implements JmxStatsWithReset {
	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private long count;
	private long totalSize;

	@SuppressWarnings("unchecked")
	StreamStatsDetailed(StreamStatsSizeCounter<?> sizeCounter) {
		this.sizeCounter = (StreamStatsSizeCounter<Object>) sizeCounter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public StreamStatsDetailed withBasicSmoothingWindow(double smoothingWindowSeconds) {
		return (StreamStatsDetailed) super.withBasicSmoothingWindow(smoothingWindowSeconds);
	}

	@Override
	public <T> StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver) {
		return sizeCounter == null ?
				item -> {
					count++;
					actualDataReceiver.onData(item);
				} :
				item -> {
					count++;
					int size = sizeCounter.size(item);
					totalSize += size;
					actualDataReceiver.onData(item);
				};
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public long getCount() {
		return count;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public Long getTotalSize() {
		return sizeCounter != null ? totalSize : null;
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public Long getTotalSizeAvg() {
		return sizeCounter != null && super.getStarted().getTotalCount() != 0 ?
				totalSize / super.getStarted().getTotalCount() :
				null;
	}

	@Override
	public void resetStats() {
		count = totalSize = 0;
		ReflectionUtils.resetStats(this);
	}
}
