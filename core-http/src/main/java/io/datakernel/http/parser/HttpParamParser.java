package io.datakernel.http.parser;

import io.datakernel.functional.Either;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.parser.HttpParamParseErrorsTree.Error;
import io.datakernel.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public interface HttpParamParser<T> {
	Either<T, HttpParamParseErrorsTree> parse(@NotNull HttpRequest request);

	@Nullable
	default T parseOrNull(@NotNull HttpRequest request) {
		return parse(request).getLeftOrNull();
	}

	default T parseOrThrow(@NotNull HttpRequest request) throws HttpParamParseException {
		Either<T, HttpParamParseErrorsTree> either = parse(request);
		if (either.isLeft()) return either.getLeft();
		throw new HttpParamParseException(either.getRight());
	}

	String getId();

	default HttpParamParser<T> withId(String id) {
		return new HttpParamParser<T>() {
			@Override
			public Either<T, HttpParamParseErrorsTree> parse(@NotNull HttpRequest request) {
				return HttpParamParser.this.parse(request);
			}

			@Override
			public String getId() {
				return id;
			}
		};
	}

	default <V> HttpParamParser<V> map(Function<T, V> fn) {
		return mapEx(HttpParamMapper.of(fn));
	}

	default <V> HttpParamParser<V> map(Function<T, V> fn, String message) {
		return mapEx(HttpParamMapper.of(fn, message));
	}

	default <V> HttpParamParser<V> mapEx(HttpParamMapper<T, V> fn) {
		return new AbstractHttpParamParser<V>(getId()) {
			@Override
			public Either<V, HttpParamParseErrorsTree> parse(@NotNull HttpRequest request) {
				return HttpParamParser.this.parse(request)
						.flatMapLeft(value ->
								fn.map(value)
										.mapRight(HttpParamParseErrorsTree::of));
			}
		};
	}

	default HttpParamParser<T> validate(Predicate<T> predicate, String error) {
		return validate(HttpParamValidator.of(predicate, error));
	}

	default HttpParamParser<T> validate(HttpParamValidator<T> validator) {
		return new AbstractHttpParamParser<T>(getId()) {
			@Override
			public Either<T, HttpParamParseErrorsTree> parse(@NotNull HttpRequest request) {
				Either<T, HttpParamParseErrorsTree> parsedValue = HttpParamParser.this.parse(request);
				if (parsedValue.isRight()) return parsedValue;
				List<Error> errors = validator.validate(parsedValue.getLeft());
				if (errors.isEmpty()) return parsedValue;
				return Either.right(HttpParamParseErrorsTree.of(errors));
			}
		};
	}

	@NotNull
	static <V> HttpParamParser<V> genericCreate(Function<Object[], V> constructor, String message, HttpParamParser<?>... parsers) {
		return genericCreateEx(HttpParamMapper.of(constructor, message), parsers);
	}

	@NotNull
	static <V> HttpParamParser<V> genericCreate(Function<Object[], V> constructor, HttpParamParser<?>... parsers) {
		return genericCreateEx(HttpParamMapper.of(constructor), parsers);
	}

	@NotNull
	static <V> HttpParamParser<V> genericCreateEx(HttpParamMapper<Object[], V> constructor, HttpParamParser<?>... parsers) {
		return new AbstractHttpParamParser<V>("") {
			@Override
			public Either<V, HttpParamParseErrorsTree> parse(@NotNull HttpRequest request) {
				Object[] args = new Object[parsers.length];
				HttpParamParseErrorsTree errors = HttpParamParseErrorsTree.create();
				for (int i = 0; i < parsers.length; i++) {
					HttpParamParser<?> parser = parsers[i];
					Either<?, HttpParamParseErrorsTree> parsed = parser.parse(request);
					if (parsed.isLeft()) {
						args[i] = parsed.getLeft();
					} else {
						errors.with(parser.getId(), parsed.getRight());
					}
				}
				if (errors.hasErrors()) {
					return Either.right(errors);
				}
				return constructor.map(args)
						.mapRight(HttpParamParseErrorsTree::of);
			}
		};
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1> HttpParamParser<R> create(TupleConstructor1<T1, R> constructor, HttpParamParser<T1> param1) {
		return genericCreate(params -> constructor.create((T1) params[0]),
				param1);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2> HttpParamParser<R> create(TupleConstructor2<T1, T2, R> constructor,
			HttpParamParser<T1> param1,
			HttpParamParser<T2> param2) {
		return genericCreate(params -> constructor.create((T1) params[0], (T2) params[2]),
				param1, param2);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3> HttpParamParser<R> create(TupleConstructor3<T1, T2, T3, R> constructor,
			HttpParamParser<T1> param1,
			HttpParamParser<T2> param2,
			HttpParamParser<T3> param3) {
		return genericCreate(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2]),
				param1, param2, param3);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4> HttpParamParser<R> create(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			HttpParamParser<T1> param1,
			HttpParamParser<T2> param2,
			HttpParamParser<T3> param3,
			HttpParamParser<T4> param4) {
		return genericCreate(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3]),
				param1, param2, param3, param4);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4, T5> HttpParamParser<R> create(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			HttpParamParser<T1> param1,
			HttpParamParser<T2> param2,
			HttpParamParser<T3> param3,
			HttpParamParser<T4> param4,
			HttpParamParser<T5> param5) {
		return genericCreate(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3], (T5) params[4]),
				param1, param2, param3, param4, param5);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4, T5, T6> HttpParamParser<R> create(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			HttpParamParser<T1> param1,
			HttpParamParser<T2> param2,
			HttpParamParser<T3> param3,
			HttpParamParser<T4> param4,
			HttpParamParser<T5> param5,
			HttpParamParser<T6> param6) {
		return genericCreate(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3], (T5) params[5], (T6) params[6]),
				param1, param2, param3, param4, param5, param6);
	}
}

