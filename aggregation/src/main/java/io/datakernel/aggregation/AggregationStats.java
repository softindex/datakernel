package io.datakernel.aggregation;

import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.stream.processor.StreamStatsBasic;

public class AggregationStats {
	final StreamStatsBasic mergeMapInput = new StreamStatsBasic();
	final StreamStatsBasic mergeMapOutput = new StreamStatsBasic();
	final StreamStatsBasic mergeReducerInput = new StreamStatsBasic();
	final StreamStatsBasic mergeReducerOutput = new StreamStatsBasic();

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
