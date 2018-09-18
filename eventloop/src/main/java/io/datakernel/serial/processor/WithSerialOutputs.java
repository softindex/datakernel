package io.datakernel.serial.processor;

import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialOutput;

public interface WithSerialOutputs<B extends WithSerialOutputs<B, T>, T> {
	SerialOutput<T> addOutput();

	default void addOutput(SerialConsumer<T> output) {
		addOutput().setOutput(output);
	}

	@SuppressWarnings("unchecked")
	default B withOutput(SerialConsumer<T> output) {
		addOutput(output);
		return (B) this;
	}
}
