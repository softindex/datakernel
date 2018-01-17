package io.datakernel.util;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public interface MutableBuilder<T extends MutableBuilder<T>> extends Builder<T> {
	default T initialize(Consumer<T> initializer) {
		initializer.accept((T) this);
		return (T) this;
	}

	default T initialize(List<? extends Consumer<T>> initializer) {
		for (Consumer<T> modifier : initializer) {
			modifier.accept((T) this);
		}
		return (T) this;
	}

	default T initialize(Consumer<T>... initializer) {
		return initialize(Arrays.asList(initializer));
	}
}
