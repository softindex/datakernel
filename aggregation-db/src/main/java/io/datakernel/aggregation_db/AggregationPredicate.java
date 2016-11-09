package io.datakernel.aggregation_db;

import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.PredicateDef;

import java.util.Map;
import java.util.Set;

public interface AggregationPredicate {

	AggregationPredicate simplify();

	Set<String> getDimensions();

	Map<String, Object> getFullySpecifiedDimensions();

	PredicateDef createPredicateDef(Expression record, FieldType.FieldConverters converters);
}
