package io.datakernel.aggregation;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.PredicateDef;

import java.util.Map;
import java.util.Set;

public interface AggregationPredicate {

	AggregationPredicate simplify();

	Set<String> getDimensions();

	Map<String, Object> getFullySpecifiedDimensions();

	PredicateDef createPredicateDef(Expression record, FieldAccessor fields);

	interface FieldAccessor {
		Object toInternalValue(String field, Object value);
	}
}
