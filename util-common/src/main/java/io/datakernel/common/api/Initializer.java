package io.datakernel.common.api;

import static java.util.Arrays.asList;

@FunctionalInterface
public interface Initializer<T> {
	void accept(T t);

	default Initializer<T> andThen(Initializer<T> next) {
		return t -> {
			this.accept(t);
			next.accept(t);
		};
	}

	static <T extends Initializable<T>> Initializer<T> empty() {
		return $ -> {};
	}

	static <T> Initializer<T> combine(Iterable<Initializer<T>> initializers) {
		return target -> initializers.forEach(initializer -> initializer.accept(target));
	}

	@SafeVarargs
	static <T> Initializer<T> combine(Initializer<T>... initializers) {
		return combine(asList(initializers));
	}
}
