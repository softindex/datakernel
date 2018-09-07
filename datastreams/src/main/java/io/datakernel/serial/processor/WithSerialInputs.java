package io.datakernel.serial.processor;

import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialSupplier;

public interface WithSerialInputs<B extends WithSerialInputs<B, T>, T> {
	SerialInput<T> addInput();

	default void addInput(SerialSupplier<T> input) {
		addInput().setInput(input);
	}

	@SuppressWarnings("unchecked")
	default B withInput(SerialSupplier<T> input) {
		addInput(input);
		return (B) this;
	}

}
