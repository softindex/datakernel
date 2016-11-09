package io.datakernel.cube.api;

import io.datakernel.aggregation_db.fieldtype.FieldType.FieldConverters;

public interface RecordFunction {
	void copyAttributes(Object result, Record record, FieldConverters converters);

	void copyMeasures(Object result, Record record, FieldConverters converters);
}
