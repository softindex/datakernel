package io.datakernel.aggregation.measure;

import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.fieldtype.FieldTypes;

public class Measures {
	private Measures() {
	}

	public static Measure sum(FieldType<?> ofType) {
		return new MeasureSum(ofType);
	}

	public static Measure min(FieldType<?> ofType) {
		return new MeasureMin(ofType);
	}

	public static Measure max(FieldType<?> ofType) {
		return new MeasureMax(ofType);
	}

	public static Measure count(FieldType<?> ofType) {
		return new MeasureCount(ofType);
	}

	public static Measure hyperLogLog(int registers) {
		return new MeasureHyperLogLog(registers);
	}

	public static Measure union(FieldType<?> fieldType) {
		return new MeasureUnion(FieldTypes.ofSet(fieldType));
	}
}
