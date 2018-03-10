package io.datakernel.util;

@FunctionalInterface
public interface Initializer<T extends Initializable<T>> {
	void accept(T t);
}
