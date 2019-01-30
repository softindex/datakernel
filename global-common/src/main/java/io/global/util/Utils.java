package io.global.util;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.functional.Try;

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
	@SuppressWarnings("Convert2MethodRef")
	public static <T> Promise<List<T>> nSuccessesOrLess(int n, Iterator<Promise<T>> promises) {
		return reduceEx(
				promises,
				() -> new ArrayList<T>(),
				a -> n - a.size(),
				(a, v) -> {
					if (v.isSuccess()) {
						a.add(v.get());
						if (a.size() == n) {
							return Try.of(a);
						}
					}
					return null;
				},
				a -> Try.of(a),
				Cancellable::tryCancel);
	}

	public static <T> Promise<Boolean> tolerantCollectBoolean(Collection<T> items, Function<T, Promise<Boolean>> fn) {
		return tolerantCollectBoolean(items.stream(), fn);
	}

	public static <T> Promise<Boolean> tolerantCollectBoolean(Stream<T> items, Function<T, Promise<Boolean>> fn) {
		return Promises.collectSequence(Try.reducer(false, (a, b) -> a || b), items.map(item -> AsyncSupplier.cast(() -> fn.apply(item).toTry())))
				.thenCompose(Promise::ofTry);
	}

	public static <T> Promise<Void> tolerantCollectVoid(Collection<T> items, Function<T, Promise<Void>> fn) {
		return tolerantCollectVoid(items.stream(), fn);
	}

	public static <T> Promise<Void> tolerantCollectVoid(Stream<T> items, Function<T, Promise<Void>> fn) {
		return Promises.collectSequence(Try.voidReducer(), items.map(item -> AsyncSupplier.cast(() -> fn.apply(item).toTry())))
				.thenCompose(Promise::ofTry);
	}
}

