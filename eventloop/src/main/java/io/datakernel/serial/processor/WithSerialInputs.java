package io.datakernel.serial.processor;

import io.datakernel.serial.SerialInput;

public interface WithSerialInputs<B, T> {
	SerialInput<T> addInput();
}
