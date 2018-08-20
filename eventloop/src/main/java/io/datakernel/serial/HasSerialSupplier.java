package io.datakernel.serial;

public interface HasSerialSupplier<T> {
	SerialSupplier<T> getSupplier();
}
