package io.datakernel.http.decoder;


import io.datakernel.functional.Either;
import io.datakernel.http.decoder.HttpDecodeErrors.Error;

import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;

@FunctionalInterface
public interface HttpMapper<T, V> {
	Either<V, List<Error>> map(T value);

	static <T, V> HttpMapper<T, V> of(Function<T, V> fn) {
		return value -> Either.left(fn.apply(value));
	}

	static <T, V> HttpMapper<T, V> of(Function<T, V> fn, String message) {
		return value -> {
			try {
				return Either.left(fn.apply(value));
			} catch (Exception e) {
				return Either.right(singletonList(Error.of(message, value)));
			}
		};
	}
}
