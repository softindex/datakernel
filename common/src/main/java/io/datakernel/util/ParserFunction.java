package io.datakernel.util;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;

import java.util.function.Function;

@FunctionalInterface
public interface ParserFunction<T, R> {
	R parse(T value) throws ParseException;

	static <T, R> Function<T, R> asFunction(ParserFunction<T, R> fn) {
		return item -> {
			try {
				return fn.parse(item);
			} catch (ParseException e) {
				throw new UncheckedException(e);
			}
		};
	}

	default R parseOrDefault(@Nullable T value, R defaultResult) throws ParseException {
		if (value != null) return parse(value);
		return defaultResult;
	}

	default <V> ParserFunction<T, V> andThen(ParserFunction<? super R, ? extends V> after) {
		return (T t) -> after.parse(parse(t));
	}

	static <T, R> R parseNullable(ParserFunction<T, R> parser, @Nullable T maybeValue, R defaultValue) throws ParseException {
		return parser.parseOrDefault(maybeValue, defaultValue);
	}

}