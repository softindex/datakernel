package io.datakernel.util;

@SuppressWarnings("unchecked")
public interface Initializable<T extends Initializable<T>> {
	default T initialize(Initializer<T> initializer) {
		initializer.accept((T) this);
		return (T) this;
	}
}
