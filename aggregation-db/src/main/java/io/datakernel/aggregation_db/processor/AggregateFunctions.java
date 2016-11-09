package io.datakernel.aggregation_db.processor;

import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.FieldTypes;

public class AggregateFunctions {
	private AggregateFunctions() {
	}

	public static AggregateFunction sum(FieldType<?> ofType) {
		return new AggregateFunctionSum(ofType);
	}

	public static AggregateFunction min(FieldType<?> ofType) {
		return new AggregateFunctionMin(ofType);
	}

	public static AggregateFunction max(FieldType<?> ofType) {
		return new AggregateFunctionMax(ofType);
	}

	public static AggregateFunction count(FieldType<?> ofType) {
		return new AggregateFunctionCount(ofType);
	}

	public static AggregateFunction hyperLogLog(int registers) {
		return new AggregateFunctionHyperLogLog(registers);
	}

	public static AggregateFunction union(FieldType<?> fieldType) {
		return new AggregateFunctionUnion(FieldTypes.ofSet(fieldType));
	}
}
