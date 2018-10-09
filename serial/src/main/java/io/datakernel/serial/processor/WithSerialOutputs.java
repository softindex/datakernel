package io.datakernel.serial.processor;

import io.datakernel.serial.SerialOutput;

public interface WithSerialOutputs<B, T> {
	SerialOutput<T> addOutput();
}
