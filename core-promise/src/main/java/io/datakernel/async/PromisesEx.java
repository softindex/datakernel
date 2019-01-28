package io.datakernel.async;

import io.datakernel.exception.StacklessException;
import io.datakernel.functional.Try;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.Promises.*;
import static java.util.Arrays.asList;

/**
 * Allows to work with multiple {@link Promise}s and
 * control which of them completed successfully.
 */
public class PromisesEx {
	private PromisesEx() {
		throw new AssertionError();
	}

	/**
	 * @see #firstSuccessfulOr(Object, Iterable)
	 */
	@SafeVarargs
	public static <T> Promise<T> firstSuccessfulOr(T defaultValue, AsyncSupplier<? extends T>... promises) {
		return firstSuccessfulOr(defaultValue, asList(promises));
	}

	/**
	 * Picks first {@code Promise} that was completed without exceptions.
	 * Returns {@code Promise} of {@code defaultValue} if none were successful.
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> firstSuccessfulOr(T defaultValue, Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return firstSuccessfulOr(defaultValue, asPromises(promises.iterator()));
	}

	/**
	 * @see #firstSuccessfulOr(Object, Iterator)
	 */
	public static <T> Promise<T> firstSuccessfulOr(T defaultValue, Stream<? extends AsyncSupplier<? extends T>> promises) {
		return firstSuccessfulOr(defaultValue, asPromises(promises.iterator()));
	}

	/**
	 * Picks first {@code Promise} that was completed without exceptions.
	 * Returns {@code Promise} of {@code defaultValue} if none were successful.
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> firstSuccessfulOr(T defaultValue, Iterator<? extends Promise<? extends T>> promises) {
		return first(isResult(), promises)
				.thenApplyEx((result, e) -> e == null ? result : defaultValue);
	}

	/**
	 * @see #firstSuccessfulOr(Promise, Iterable)
	 */
	@SafeVarargs
	public static <T> Promise<T> firstSuccessfulOr(Promise<T> defaultPromise, AsyncSupplier<? extends T>... promises) {
		return firstSuccessfulOr(defaultPromise, asList(promises));
	}

	/**
	 * @see #firstSuccessfulOr(Promise, Iterator)
	 */
	public static <T> Promise<T> firstSuccessfulOr(Promise<T> defaultPromise, Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return firstSuccessfulOr(defaultPromise, asPromises(promises.iterator()));
	}

	/**
	 * @see #firstSuccessfulOr(Promise, Iterator)
	 */
	public static <T> Promise<T> firstSuccessfulOr(Promise<T> defaultPromise, Stream<? extends AsyncSupplier<? extends T>> promises) {
		return firstSuccessfulOr(defaultPromise, asPromises(promises.iterator()));
	}

	/**
	 * Picks first {@code Promise} that was completed without exceptions.
	 * Returns {@code defaultPromise} if none were successful.
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> firstSuccessfulOr(Promise<T> defaultPromise, Iterator<? extends Promise<? extends T>> promises) {
		return first(isResult(), promises)
				.thenComposeEx((result, e) -> e == null ? Promise.of(result) : defaultPromise);
	}

	/**
	 * @see #nSuccesses(int, Iterator)
	 */
	public static <T> Promise<List<T>> nSuccesses(int n, Stream<? extends AsyncSupplier<? extends T>> promises) {
		return nSuccesses(n, asPromises(promises.iterator()));
	}

	/**
	 * Returns a {@code List} of successfully completed {@code Promise}s.
	 * Length of returned {@code List} must be strictly {@code n}.
	 */
	public static <T> Promise<List<T>> nSuccesses(int n, Iterator<Promise<T>> promises) {
		SettablePromise<List<T>> result = new SettablePromise<>();
		List<T> results = new ArrayList<>();
		List<Throwable> errors = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			oneSuccessInto(promises, n, result, results, errors);
		}
		return result;
	}

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
		SettablePromise<List<T>> result = new SettablePromise<>();
		List<T> results = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			oneSuccessInto(promises, n, result, results, null);
		}
		return result;
	}

	private static <T> void oneSuccessInto(Iterator<Promise<T>> promises, int n, SettablePromise<List<T>> result, List<T> results, @Nullable List<Throwable> errors) {
		if (!promises.hasNext()) {
			if (errors == null) {
				result.set(results);
			} else {
				if (!results.isEmpty() && results.get(0) instanceof Cancellable) {
					results.forEach(r -> ((Cancellable) r).cancel());
				}
				StacklessException e = new StacklessException(Promises.class, "Not enough successes");
				errors.forEach(e::addSuppressed);
				result.setException(e);
			}
			return;
		}
		promises.next()
				.whenComplete((res, e) -> {
					if (e != null) {
						if (errors != null) {
							errors.add(e);
						}
						oneSuccessInto(promises, n, result, results, errors);
						return;
					}
					results.add(res);
					if (results.size() >= n) {
						result.set(results);
					}
				});
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
