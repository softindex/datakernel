package io.datakernel.http.parser;

import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public interface HttpParamParser<T> {
	T parse(@NotNull HttpRequest request) throws ParseException;

	Set<String> getIds();

	default HttpParamParser<T> withIds(Set<String> ids) {
		return new HttpParamParser<T>() {
			@Override
			public T parse(@NotNull HttpRequest request) throws ParseException {
				return HttpParamParser.this.parse(request);
			}

			@Override
			public Set<String> getIds() {
				return ids;
			}
		};
	}

	default HttpParamParser<T> withId(String id) {
		return withIds(Collections.singleton(id));
	}

	@Nullable
	default T parseOrNull(@NotNull HttpRequest request) {
		try {
			return parse(request);
		} catch (ParseException e) {
			return null;
		}
	}

	default <R> HttpParamParser<R> map(Function<T, R> fn) {
		return new HttpParamParser<R>() {
			@Override
			public R parse(@NotNull HttpRequest request) throws ParseException {
				T parsedValue = HttpParamParser.this.parse(request);
				if (parsedValue == null) return null;
				try {
					return fn.apply(parsedValue);
				} catch (Exception e) {
					throw new ParseException(String.format("Parse exception on field %s", getIds()), e);
				}
			}

			@Override
			public Set<String> getIds() {
				return HttpParamParser.this.getIds();
			}
		};
	}

	default HttpParamParser<T> validate(Predicate<T> predicate) {
		return new HttpParamParser<T>() {
			@Override
			public T parse(@NotNull HttpRequest request) throws ParseException {
				T parsedValue = HttpParamParser.this.parse(request);
				if (parsedValue == null) return null;
				if (predicate.test(parsedValue)) {
					return parsedValue;
				}
				throw new ParseException(
						String.format("Invalidate predicate for the field %s", HttpParamParser.this.getIds()));
			}

			@Override
			public Set<String> getIds() {
				return HttpParamParser.this.getIds();
			}
		};
	}

	@NotNull
	static <R, T1> HttpParamParser<R> create(TupleParser1<T1, R> constructor, HttpParamParser<T1> param1) {
		return new HttpParamParserImpl<R>() {
			@Override
			public R parse(@NotNull HttpRequest request) throws ParseException {
				return constructor.create(param1.parseOrNull(request));
			}
		};
	}

	@NotNull
	static <R, T1, T2> HttpParamParser<R> create(TupleParser2<T1, T2, R> constructor,
												 HttpParamParser<T1> param1,
												 HttpParamParser<T2> param2) {
		return new HttpParamParserImpl<R>() {
			@Override
			public R parse(@NotNull HttpRequest request) throws ParseException {
				return constructor.create(
						param1.parse(request),
						param2.parse(request));
			}
		};
	}

	@NotNull
	static <R, T1, T2, T3> HttpParamParser<R> create(TupleParser3<T1, T2, T3, R> constructor,
													 HttpParamParser<T1> param1,
													 HttpParamParser<T2> param2,
													 HttpParamParser<T3> param3) {
		return new HttpParamParserImpl<R>() {
			@Override
			public R parse(@NotNull HttpRequest request) throws ParseException {
				return constructor.create(
						param1.parse(request),
						param2.parse(request),
						param3.parse(request));
			}
		};
	}

	@NotNull
	static <R, T1, T2, T3, T4> HttpParamParser<R> create(TupleParser4<T1, T2, T3, T4, R> constructor,
														 HttpParamParser<T1> param1,
														 HttpParamParser<T2> param2,
														 HttpParamParser<T3> param3,
														 HttpParamParser<T4> param4) {
		return new HttpParamParserImpl<R>() {
			@Override
			public R parse(@NotNull HttpRequest request) throws ParseException {
				return constructor.create(
						param1.parse(request),
						param2.parse(request),
						param3.parse(request),
						param4.parse(request));
			}
		};
	}

	@NotNull
	static <R, T1, T2, T3, T4, T5> HttpParamParser<R> create(TupleParser5<T1, T2, T3, T4, T5, R> constructor,
															 HttpParamParser<T1> param1,
															 HttpParamParser<T2> param2,
															 HttpParamParser<T3> param3,
															 HttpParamParser<T4> param4,
															 HttpParamParser<T5> param5) {
		return new HttpParamParserImpl<R>() {
			@Override
			public R parse(@NotNull HttpRequest request) throws ParseException {
				return constructor.create(
						param1.parse(request),
						param2.parse(request),
						param3.parse(request),
						param4.parse(request),
						param5.parse(request));
			}
		};
	}

	@NotNull
	static <R, T1, T2, T3, T4, T5, T6> HttpParamParser<R> create(TupleParser6<T1, T2, T3, T4, T5, T6, R> constructor,
																 HttpParamParser<T1> param1,
																 HttpParamParser<T2> param2,
																 HttpParamParser<T3> param3,
																 HttpParamParser<T4> param4,
																 HttpParamParser<T5> param5,
																 HttpParamParser<T6> param6) {
		return new HttpParamParserImpl<R>() {
			@Override
			public R parse(@NotNull HttpRequest request) throws ParseException {
				return constructor.create(
						param1.parse(request),
						param2.parse(request),
						param3.parse(request),
						param4.parse(request),
						param5.parse(request),
						param6.parse(request));
			}
		};
	}
}

