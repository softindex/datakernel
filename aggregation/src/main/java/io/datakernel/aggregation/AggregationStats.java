package io.datakernel.aggregation;

import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;

public class AggregationStats {
	final StreamStatsBasic<?> mergeMapInput = StreamStats.basic();
	final StreamStatsBasic<?> mergeMapOutput = StreamStats.basic();
	final StreamStatsBasic<?> mergeReducerInput = StreamStats.basic();
	final StreamStatsBasic<?> mergeReducerOutput = StreamStats.basic();

	@JmxAttribute
	public StreamStatsBasic getMergeReducerInput() {
		return mergeReducerInput;
	}

	@JmxAttribute
	public StreamStatsBasic getMergeReducerOutput() {
		return mergeReducerOutput;
	}

	@JmxAttribute
	public StreamStatsBasic getMergeMapInput() {
		return mergeMapInput;
	}

	@JmxAttribute
	public StreamStatsBasic getMergeMapOutput() {
		return mergeMapOutput;
	}
}
