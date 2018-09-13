package io.datakernel.serial;

@FunctionalInterface
public interface SerialConsumerFunction<T, R> {
	R apply(SerialConsumer<T> consumer);

	static <T> SerialConsumerFunction<T, SerialConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
