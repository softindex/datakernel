package io.datakernel.util;

import static java.util.Arrays.asList;

@FunctionalInterface
public interface Initializer<T extends Initializable<T>> {
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

	static <T extends Initializable<T>> Initializer<T> combine(Iterable<? extends Initializer<? super T>> initializers) {
		return target -> initializers.forEach(initializer -> initializer.accept(target));
	}

	@SafeVarargs
	static <T extends Initializable<T>, S extends Initializer<? super T>> Initializer<T> combine(S... initializers) {
		return combine(asList(initializers));
	}
}
