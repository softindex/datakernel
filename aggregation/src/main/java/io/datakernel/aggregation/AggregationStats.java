package io.datakernel.aggregation;

import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.stream.processor.StreamStats;
import io.datakernel.stream.processor.StreamStatsBasic;

public class AggregationStats {
	final StreamStatsBasic mergeMapInput = StreamStats.basic();
	final StreamStatsBasic mergeMapOutput = StreamStats.basic();
	final StreamStatsBasic mergeReducerInput = StreamStats.basic();
	final StreamStatsBasic mergeReducerOutput = StreamStats.basic();

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

	@JmxOperation
	public void resetStats() {
		mergeReducerInput.resetStats();
		mergeReducerOutput.resetStats();
		mergeMapInput.resetStats();
		mergeMapOutput.resetStats();
	}
}
