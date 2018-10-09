package io.datakernel.serial.processor;

import io.datakernel.serial.HasSerialOutput;
import io.datakernel.serial.SerialConsumer;

public interface WithSerialOutput<B, T> extends HasSerialOutput<T> {
	@SuppressWarnings("unchecked")
	default B withOutput(SerialConsumer<T> output) {
		getOutput().set(output);
		return (B) this;
	}
}
