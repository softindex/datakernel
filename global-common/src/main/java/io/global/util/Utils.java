package io.global.util;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.async.process.Cancellable;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.common.collection.Try;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.SettablePromise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.promise.Promises.asPromises;
import static io.datakernel.promise.Promises.reduceEx;
import static io.global.common.BinaryDataFormats.REGISTRY;

public class Utils {
	public static final StructuredCodec<Map<PubKey, Set<RawServerId>>> PUB_KEYS_MAP =
			StructuredCodecs.ofMap(REGISTRY.get(PubKey.class), StructuredCodecs.ofSet(REGISTRY.get(RawServerId.class)));
	public static final StructuredCodec<Map<PubKey, Set<RawServerId>>> PUB_KEYS_MAP_HEX =
			StructuredCodecs.ofMap(STRING_CODEC.transform(PubKey::fromString, PubKey::asString), StructuredCodecs.ofSet(REGISTRY.get(RawServerId.class)));

	private Utils() {
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

	public static boolean arrayStartsWith(byte[] array, byte[] prefix) {
		if (array.length < prefix.length) return false;
		for (int i = 0; i < prefix.length; i++) {
			if (array[i] != prefix[i]) return false;
		}
		return true;
	}

	@NotNull
	public static String limit(@NotNull String string, int maxLength) {
		return string.substring(0, Math.min(maxLength, string.length()));
	}

	public static Promise<Void> untilTrue(AsyncSupplier<Boolean> supplier){
		return Promises.until(false, $ -> supplier.get(), done -> done).toVoid();
	}

	public static <T> Promise<T> until(AsyncSupplier<T> supplier, Predicate<T> breakCondition) {
		return Promises.until(null, $ -> supplier.get(), breakCondition);
	}

}

