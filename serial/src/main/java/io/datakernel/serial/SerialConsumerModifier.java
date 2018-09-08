package io.datakernel.serial;

@FunctionalInterface
public interface SerialConsumerModifier<T, R> {
	SerialConsumer<R> apply(SerialConsumer<T> consumer);

	static <T> SerialConsumerModifier<T, T> identity() {
		return consumer -> consumer;
	}
}
