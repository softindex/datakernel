package io.datakernel.aggregation_db;

import io.datakernel.aggregation_db.processor.AggregateFunction;

import java.util.List;

public interface HasAggregationStructure extends HasAggregationTypes {
	AggregateFunction getFieldAggregateFunction(String field);

	List<String> getPartitioningKey();
}
