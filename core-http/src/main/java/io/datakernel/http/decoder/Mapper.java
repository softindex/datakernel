package io.datakernel.http.decoder;


import io.datakernel.functional.Either;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * An enhanced mapping function which can return a list of errors for given input object.
 * This can be used to both map and put additional constraints on the parsed object from HTTP decoder.
 * For example to ensure, that age of the person given as a string is <b>an integer</b> in range 0-100
 * and convert it to that.
 */
@FunctionalInterface
public interface Mapper<T, V> {
	Either<V, List<DecodeError>> map(T value);

	static <T, V> Mapper<T, V> of(Function<T, V> fn) {
		return value -> Either.left(fn.apply(value));
	}

	static <T, V> Mapper<T, V> of(Function<T, V> fn, String message) {
		return value -> {
			try {
				return Either.left(fn.apply(value));
			} catch (Exception e) {
				return Either.right(singletonList(DecodeError.of(message, value)));
			}
		};
	}
}
