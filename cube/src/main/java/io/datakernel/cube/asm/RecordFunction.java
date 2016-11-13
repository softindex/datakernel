package io.datakernel.cube.asm;

import io.datakernel.cube.Record;

public interface RecordFunction {
	void copyAttributes(Object result, Record record);

	void copyMeasures(Object result, Record record);
}
