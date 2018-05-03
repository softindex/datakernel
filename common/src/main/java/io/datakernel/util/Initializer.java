package io.datakernel.util;

import java.util.List;

import static java.util.Arrays.asList;

@FunctionalInterface
public interface Initializer<T extends Initializable<T>> {
	void accept(T t);

	static <T extends Initializable<T>> Initializer<T> empty() {
		return $ -> {};
	}

	static <T extends Initializable<T>> Initializer<T> combine(List<? extends Initializer<? super T>> initializers) {
		return target -> initializers.forEach(initializer -> initializer.accept(target));
	}

	@SafeVarargs
	static <T extends Initializable<T>> Initializer<T> combine(Initializer<? super T>... initializers) {
		return combine(asList(initializers));
	}
}
