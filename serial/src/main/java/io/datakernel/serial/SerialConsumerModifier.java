package io.datakernel.serial;

@FunctionalInterface
public interface SerialConsumerModifier<T, R> {
	R apply(SerialConsumer<T> consumer);

	static <T> SerialConsumerModifier<T, SerialConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
