package io.datakernel.http.decoder;

import io.datakernel.functional.Either;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.decoder.HttpDecodeErrors.Error;
import io.datakernel.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public interface HttpDecoder<T> {
	Either<T, HttpDecodeErrors> decode(@NotNull HttpRequest request);

	@Nullable
	default T decodeOrNull(@NotNull HttpRequest request) {
		return decode(request).getLeftOrNull();
	}

	default T decodeOrThrow(@NotNull HttpRequest request) throws HttpDecodeException {
		Either<T, HttpDecodeErrors> either = decode(request);
		if (either.isLeft()) return either.getLeft();
		throw new HttpDecodeException(either.getRight());
	}

	String getId();

	default HttpDecoder<T> withId(String id) {
		return new HttpDecoder<T>() {
			@Override
			public Either<T, HttpDecodeErrors> decode(@NotNull HttpRequest request) {
				return HttpDecoder.this.decode(request);
			}

			@Override
			public String getId() {
				return id;
			}
		};
	}

	default <V> HttpDecoder<V> map(Function<T, V> fn) {
		return mapEx(HttpMapper.of(fn));
	}

	default <V> HttpDecoder<V> map(Function<T, V> fn, String message) {
		return mapEx(HttpMapper.of(fn, message));
	}

	default <V> HttpDecoder<V> mapEx(HttpMapper<T, V> fn) {
		return new AbstractHttpDecoder<V>(getId()) {
			@Override
			public Either<V, HttpDecodeErrors> decode(@NotNull HttpRequest request) {
				return HttpDecoder.this.decode(request)
						.flatMapLeft(value ->
								fn.map(value)
										.mapRight(HttpDecodeErrors::of));
			}
		};
	}

	default HttpDecoder<T> validate(Predicate<T> predicate, String error) {
		return validate(HttpValidator.of(predicate, error));
	}

	default HttpDecoder<T> validate(HttpValidator<T> validator) {
		return new AbstractHttpDecoder<T>(getId()) {
			@Override
			public Either<T, HttpDecodeErrors> decode(@NotNull HttpRequest request) {
				Either<T, HttpDecodeErrors> decodedValue = HttpDecoder.this.decode(request);
				if (decodedValue.isRight()) return decodedValue;
				List<Error> errors = validator.validate(decodedValue.getLeft());
				if (errors.isEmpty()) return decodedValue;
				return Either.right(HttpDecodeErrors.of(errors));
			}
		};
	}

	@NotNull
	static <V> HttpDecoder<V> create(Function<Object[], V> constructor, String message, HttpDecoder<?>... decoders) {
		return createEx(HttpMapper.of(constructor, message), decoders);
	}

	@NotNull
	static <V> HttpDecoder<V> create(Function<Object[], V> constructor, HttpDecoder<?>... decoders) {
		return createEx(HttpMapper.of(constructor), decoders);
	}

	@NotNull
	static <V> HttpDecoder<V> createEx(HttpMapper<Object[], V> constructor, HttpDecoder<?>... decoders) {
		return new AbstractHttpDecoder<V>("") {
			@Override
			public Either<V, HttpDecodeErrors> decode(@NotNull HttpRequest request) {
				Object[] args = new Object[decoders.length];
				HttpDecodeErrors errors = HttpDecodeErrors.create();
				for (int i = 0; i < decoders.length; i++) {
					HttpDecoder<?> decoder = decoders[i];
					Either<?, HttpDecodeErrors> decoded = decoder.decode(request);
					if (decoded.isLeft()) {
						args[i] = decoded.getLeft();
					} else {
						errors.with(decoder.getId(), decoded.getRight());
					}
				}
				if (errors.hasErrors()) {
					return Either.right(errors);
				}
				return constructor.map(args)
						.mapRight(HttpDecodeErrors::of);
			}
		};
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1> HttpDecoder<R> of(TupleConstructor1<T1, R> constructor, HttpDecoder<T1> param1) {
		return create(params -> constructor.create((T1) params[0]),
				param1);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2> HttpDecoder<R> of(TupleConstructor2<T1, T2, R> constructor,
			HttpDecoder<T1> param1,
			HttpDecoder<T2> param2) {
		return create(params -> constructor.create((T1) params[0], (T2) params[2]),
				param1, param2);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3> HttpDecoder<R> of(TupleConstructor3<T1, T2, T3, R> constructor,
			HttpDecoder<T1> param1,
			HttpDecoder<T2> param2,
			HttpDecoder<T3> param3) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2]),
				param1, param2, param3);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4> HttpDecoder<R> of(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			HttpDecoder<T1> param1,
			HttpDecoder<T2> param2,
			HttpDecoder<T3> param3,
			HttpDecoder<T4> param4) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3]),
				param1, param2, param3, param4);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4, T5> HttpDecoder<R> of(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			HttpDecoder<T1> param1,
			HttpDecoder<T2> param2,
			HttpDecoder<T3> param3,
			HttpDecoder<T4> param4,
			HttpDecoder<T5> param5) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3], (T5) params[4]),
				param1, param2, param3, param4, param5);
	}

	@SuppressWarnings("unchecked")
	@NotNull
	static <R, T1, T2, T3, T4, T5, T6> HttpDecoder<R> of(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			HttpDecoder<T1> param1,
			HttpDecoder<T2> param2,
			HttpDecoder<T3> param3,
			HttpDecoder<T4> param4,
			HttpDecoder<T5> param5,
			HttpDecoder<T6> param6) {
		return create(params -> constructor.create((T1) params[0], (T2) params[1], (T3) params[2], (T4) params[3], (T5) params[5], (T6) params[6]),
				param1, param2, param3, param4, param5, param6);
	}
}

