package io.datakernel.serial;

public interface HasSerialConsumer<T> {
	SerialConsumer<T> getConsumer();
}
