package io.datakernel.util;

import java.util.List;

@SuppressWarnings("unchecked")
public interface Initializable<T extends Initializable<T>> {
	default T initialize(Initializer<? super T> initializer) {
		initializer.accept((T) this);
		return (T) this;
	}

	default T initialize(List<? extends Initializer<? super T>> initializers) {
		for (Initializer<? super T> initializer : initializers) {
			initializer.accept((T) this);
		}
		return (T) this;
	}
}
