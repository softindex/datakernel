package io.datakernel.util;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedWrapperException;

public interface ParserFunction<T, R> {
	R doParse(T t) throws ParseException;

	default R parse(T t) throws ParseException {
		try {
			return doParse(t);
		} catch (UncheckedWrapperException e) {
			Throwable cause = e.getCause();
			if (cause instanceof ParseException) {
				throw (ParseException) cause;
			}
			throw new ParseException(cause);
		}
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
