package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialOutput;

public interface WithSerialOutputs<B, T> {
	SerialOutput<T> addOutput();

	default void addOutput(SerialConsumer<T> output) {
		addOutput().set(output);
	}

	@SuppressWarnings("unchecked")
	default B withOutput(SerialConsumer<T> output) {
		addOutput(output);
		return (B) this;
	}
}
