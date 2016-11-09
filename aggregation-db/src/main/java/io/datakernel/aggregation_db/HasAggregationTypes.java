package io.datakernel.aggregation_db;

import io.datakernel.aggregation_db.fieldtype.FieldType;

import java.util.List;
import java.util.Map;

public interface HasAggregationTypes {
	List<String> getKeys();

	List<String> getFields();

	Map<String, FieldType> getKeyTypes();

	Map<String, FieldType> getFieldTypes();

	FieldType getKeyType(String key);

	FieldType getFieldType(String field);
}
