package io.datakernel.datastream;

@FunctionalInterface
public interface StreamSupplierTransformer<T, R> {
	R transform(StreamSupplier<T> supplier);

	static <T> StreamSupplierTransformer<T, StreamSupplier<T>> identity() {
		return supplier -> supplier;
	}
}
