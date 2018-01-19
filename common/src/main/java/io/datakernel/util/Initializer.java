package io.datakernel.util;

import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public interface Initializer<T extends Initializer<T>> extends Modifier<T> {
	default T initialize(Consumer<T> initializer) {
		initializer.accept((T) this);
		return (T) this;
	}

	default T initialize(List<? extends Consumer<T>> initializers) {
		for (Consumer<T> initializer : initializers) {
			initializer.accept((T) this);
		}
		return (T) this;
	}
}
