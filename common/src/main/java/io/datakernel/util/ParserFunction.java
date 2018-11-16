package io.datakernel.util;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.ParseException;

@FunctionalInterface
public interface ParserFunction<T, R> {
	R parse(T value) throws ParseException;

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
