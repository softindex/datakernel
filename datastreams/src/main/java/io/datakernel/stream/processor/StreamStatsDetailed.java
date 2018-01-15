package io.datakernel.stream.processor;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.stream.StreamDataReceiver;

public final class StreamStatsDetailed extends StreamStatsBasic {
	@Nullable
	private final StreamStatsSizeCounter<Object> sizeCounter;

	private long count;
	private long totalSize;

	@SuppressWarnings("unchecked")
	private StreamStatsDetailed(StreamStatsSizeCounter<?> sizeCounter) {
		this.sizeCounter = (StreamStatsSizeCounter<Object>) sizeCounter;
	}

	public static StreamStatsDetailed create(StreamStatsSizeCounter<?> sizeCounter) {
		return new StreamStatsDetailed(sizeCounter);
	}

	public static StreamStatsDetailed create() {
		return new StreamStatsDetailed(null);
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
	public long getTotalSize() {
		return totalSize;
	}

	@Override
	@JmxOperation
	public void resetStats() {
		super.resetStats();
		count = totalSize = 0;
	}
}
