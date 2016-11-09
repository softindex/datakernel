package io.datakernel.cube.asm;

import io.datakernel.aggregation.AggregationPredicate.FieldAccessor;
import io.datakernel.cube.Record;

public interface RecordFunction {
	void copyAttributes(Object result, Record record, FieldAccessor converters);

	void copyMeasures(Object result, Record record, FieldAccessor converters);
}
