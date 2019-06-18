package io.global.util;

import io.datakernel.async.*;
import io.datakernel.functional.Try;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.Promises.asPromises;
import static io.datakernel.async.Promises.reduceEx;

public class Utils {
	private Utils() {}

	/**
	 * @see #nSuccessesOrLess(int, Iterator)
	 */
	public static <T> Promise<List<T>> nSuccessesOrLess(int n, Stream<? extends AsyncSupplier<? extends T>> promises) {
		return nSuccessesOrLess(n, asPromises(promises.iterator()));
	}

	/**
	 * Returns a {@code List} of successfully completed {@code Promise}s.
	 * Length of returned {@code List} can't be greater than {@code n}.
	 */
	public static <T> Promise<List<T>> nSuccessesOrLess(int n, Iterator<Promise<T>> promises) {
		return reduceEx(promises, a -> n - a.size(),
				new ArrayList<T>(),
				(a, v) -> {
					if (v.isSuccess()) {
						a.add(v.get());
						if (a.size() == n) {
							return Try.of(a);
						}
					}
					return null;
				},
				Try::of,
				Cancellable::tryCancel);
	}

	public static <T> Promise<Boolean> tolerantCollectBoolean(Collection<T> items, Function<T, Promise<Boolean>> fn) {
		return tolerantCollectBoolean(items.stream(), fn);
	}

	public static <T> Promise<Boolean> tolerantCollectBoolean(Stream<T> items, Function<T, Promise<Boolean>> fn) {
		return Promises.reduce(Try.reducer(false, (a, b) -> a || b), 1,
				asPromises(items.map(item -> AsyncSupplier.cast(() -> fn.apply(item).toTry()))))
				.then(Promise::ofTry);
	}

	public static <T> Promise<Void> tolerantCollectVoid(Collection<T> items, Function<T, Promise<Void>> fn) {
		return tolerantCollectVoid(items.stream(), fn);
	}

	public static <T> Promise<Void> tolerantCollectVoid(Stream<T> items, Function<T, Promise<Void>> fn) {
		return Promises.reduce(Try.voidReducer(), 1,
				asPromises(items.map(item -> AsyncSupplier.cast(() -> fn.apply(item).toTry()))))
				.then(Promise::ofTry);
	}

	@NotNull
	public static <T> Promise<T> eitherComplete(@NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		SettablePromise<T> result = new SettablePromise<>();
		promise1.whenComplete(result::trySet);
		promise2.whenComplete(result::trySet);
		return result;
	}
}

