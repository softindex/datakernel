package io.datakernel.aggregation;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.stream.StreamProducer;

public interface IAggregation {
	double estimateCost(AggregationQuery query);

	<T> StreamProducer<T> query(AggregationQuery query, Class<T> outputClass, DefiningClassLoader classLoader);
}
