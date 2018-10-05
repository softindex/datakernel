package io.datakernel.serial;

public interface HasSerialOutput<T> {
	SerialOutput<T> getOutput();
}
