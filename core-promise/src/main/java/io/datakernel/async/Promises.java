/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.async;

import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.StacklessException;
import io.datakernel.functional.Try;
import io.datakernel.util.*;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.CollectionUtils.transformIterator;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Allows to manage multiple {@link Promise}s.
 */
public final class Promises {

	private Promises() {
	}

	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException(Promises.class, "Promise timeout");

	/**
	 * Waits until the delay passes and if the {@code Promise} is still
	 * not complete, tries to complete it with {@code TIMEOUT_EXCEPTION}.
	 *
	 * @param promise the Promise to be tracked
	 * @param delay   time of delay
	 * @return {@code Promise}
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> timeout(@NotNull Promise<T> promise, long delay) {
		if (promise.isComplete()) return promise;
		if (delay <= 0) return Promise.ofException(TIMEOUT_EXCEPTION);
		return promise.then(new NextPromise<T, T>() {
			@NotNull ScheduledRunnable schedule = getCurrentEventloop().delay(delay, () -> tryCompleteExceptionally(TIMEOUT_EXCEPTION));

			@Override
			public void accept(T result, @Nullable Throwable e) {
				if (e == null) {
					schedule.cancel();
					tryComplete(result);
				} else {
					schedule.cancel();
					tryCompleteExceptionally(e);
				}
			}
		});
	}

	/**
	 * @see #timeout(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> timeout(@NotNull Promise<T> promise, @NotNull Duration delay) {
		return timeout(promise, delay.toMillis());
	}

	/**
	 * Delays completion of provided {@code promise} for
	 * the defined period of time.
	 *
	 * @param promise     the {@code Promise} to be delayed
	 * @param delayMillis delay in millis
	 * @return completed {@code Promise}
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> delay(@NotNull Promise<T> promise, long delayMillis) {
		MaterializedPromise<T> materializedPromise = promise.materialize();
		return Promise.ofCallback(cb -> getCurrentEventloop().delay(delayMillis, () -> materializedPromise.whenComplete(cb::set)));
	}

	/**
	 * @see #delay(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> delay(@NotNull Promise<T> promise, @NotNull Duration delay) {
		return delay(promise, delay.toMillis());
	}

	/**
	 * Schedules completion of the {@code Promise} so that it will
	 * be completed after the timestamp even if its operations
	 * were completed earlier.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> schedule(@NotNull Promise<T> promise, long timestamp) {
		MaterializedPromise<T> materializedPromise = promise.materialize();
		return Promise.ofCallback(cb -> getCurrentEventloop().schedule(timestamp, () -> materializedPromise.whenComplete(cb::set)));
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> schedule(@NotNull Promise<T> promise, @NotNull Instant instant) {
		return schedule(promise, instant.toEpochMilli());
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all() {
		return Promise.complete();
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Promise<?> promise1) {
		return promise1.toVoid();
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Promise<?> promise1, @NotNull Promise<?> promise2) {
		return promise1.both(promise2);
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Promise<?>... promises) {
		return all(asIterator(promises));
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Stream<? extends Promise<?>> promises) {
		return all(promises.iterator());
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Iterable<? extends Promise<?>> promises) {
		return all(promises.iterator());
	}

	/**
	 * Returns a {@code Promise} that completes when
	 * all of the {@code promises} are completed.
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull List<? extends Promise<?>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.complete();
		if (size == 1) return promises.get(0).toVoid();
		if (size == 2) return promises.get(0).both(promises.get(1));
		return all(promises.iterator());
	}

	/**
	 * Returns {@code Promise} that completes when all of the {@code promises}
	 * are completed. If at least one of the {@code promises} completes
	 * exceptionally, a {@link CompleteExceptionallyPromise} will be returned.
	 */
	@NotNull
	public static Promise<Void> all(@NotNull Iterator<? extends Promise<?>> promises) {
		if (!promises.hasNext()) return all();
		@NotNull PromiseAll<Object> resultPromise = new PromiseAll<>();
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) continue;
			if (promise.isException()) return Promise.ofException(promise.materialize().getException());
			resultPromise.countdown++;
			promise.then(resultPromise);
		}
		return resultPromise.countdown != 0 ? resultPromise : Promise.complete();
	}

	/**
	 * Returns a {@link CompleteExceptionallyPromise} with {@link StacklessException},
	 * since this method doesn't accept any {@code Promise}s
	 *
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any() {
		return Promise.ofException(new StacklessException(Promises.class, "All promises completed exceptionally"));
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(@NotNull Promise<? extends T> promise1) {
		return (Promise<T>) promise1;
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see #any(Iterator)
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		return ((Promise<T>) promise1).either(promise2);
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	@SafeVarargs
	public static <T> Promise<T> any(@NotNull Promise<? extends T>... promises) {
		return any(asIterator(promises));
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull Stream<? extends Promise<? extends T>> promises) {
		return any(promises.iterator());
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull Iterable<? extends Promise<? extends T>> promises) {
		return any(promises.iterator());
	}

	/**
	 * @see #any(Iterator)
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 1) return (Promise<T>) promises.get(0);
		if (size == 2) return ((Promise<T>) promises.get(0)).either(promises.get(1));
		return any(promises.iterator());
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return any(promises, $ -> {});
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull Stream<? extends Promise<? extends T>> promises, @NotNull Consumer<T> cleanup) {
		return any(promises.iterator(), cleanup);
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull Iterable<? extends Promise<? extends T>> promises, @NotNull Consumer<T> cleanup) {
		return any(promises.iterator(), cleanup);
	}

	/**
	 * Returns one of the first completed {@code Promise}s. Since it's
	 * async, we can't really get the FIRST completed {@code Promise}.
	 *
	 * @return one of the first completed {@code Promise}s
	 */
	@NotNull
	public static <T> Promise<T> any(@NotNull Iterator<? extends Promise<? extends T>> promises, @NotNull Consumer<T> cleanup) {
		if (!promises.hasNext()) return any();
		@NotNull PromiseAny<T> resultPromise = new PromiseAny<>();
		while (promises.hasNext()) {
			Promise<? extends T> promise = promises.next();
			if (promise.isResult()) return Promise.of(promise.materialize().getResult());
			if (promise.isException()) continue;
			resultPromise.errors++;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					if (resultPromise.isComplete()) {
						cleanup.accept(result);
					} else {
						resultPromise.complete(result);
					}
				} else {
					if (--resultPromise.errors == 0) {
						resultPromise.completeExceptionally(e);
					}
				}
			});
		}
		return resultPromise.errors != 0 ? resultPromise : any();
	}

	/**
	 * Returns a successfully completed {@code Promise}
	 * with an empty list as the result.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList() {
		return Promise.of(emptyList());
	}

	/**
	 * Returns a completed {@code Promise}
	 * with a result wrapped in {@code List}.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Promise<? extends T> promise1) {
		return promise1.thenApply(Collections::singletonList);
	}

	/**
	 * Returns {@code Promise} with a list of {@code promise1} and {@code promise2} results.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		return promise1.combine(promise2, (value1, value2) -> asList(value1, value2));
	}

	/**
	 * @see Promises#toList(List)
	 */
	@SafeVarargs
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Promise<? extends T>... promises) {
		return toList(asList(promises));
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Stream<? extends Promise<? extends T>> promises) {
		List<Promise<? extends T>> list = promises.collect(Collectors.toList());
		return toList(list);
	}

	/**
	 * Reduces list of {@code Promise}s into Promise&lt;List&gt;.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.of(Collections.emptyList());
		if (size == 1) return promises.get(0).thenApply(Collections::singletonList);
		if (size == 2) return promises.get(0).combine(promises.get(1), Arrays::asList);

		@SuppressWarnings("unchecked") PromiseToList<T> resultPromise = new PromiseToList<>((T[]) new Object[size]);

		for (int i = 0; i < size; i++) {
			Promise<? extends T> promise = promises.get(i);
			if (promise.isResult()) {
				resultPromise.accumulator[i] = promise.materialize().getResult();
				continue;
			}
			if (promise.isException()) return Promise.ofException(promise.materialize().getException());
			int index = i;
			resultPromise.countdown++;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					resultPromise.processComplete(result, index);
				} else {
					resultPromise.tryCompleteExceptionally(e);
				}
			});
		}
		return resultPromise.countdown != 0 ? resultPromise : Promise.of(asList(resultPromise.accumulator));
	}

	/**
	 * Returns an array of provided {@code type} and length 0
	 * wrapped in {@code Promise}.
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type) {
		return Promise.of((T[]) Array.newInstance(type, 0));
	}

	/**
	 * Returns an array with {@code promise1} result.
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Promise<? extends T> promise1) {
		return promise1.thenApply(value -> {
			@NotNull T[] array = (T[]) Array.newInstance(type, 1);
			array[0] = value;
			return array;
		});
	}

	/**
	 * Returns an array with {@code promise1} and {@code promise2} results.
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		return promise1.combine(promise2, (value1, value2) -> {
			@NotNull T[] array = (T[]) Array.newInstance(type, 2);
			array[0] = value1;
			array[1] = value2;
			return array;
		});
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@SafeVarargs
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Promise<? extends T>... promises) {
		return toArray(type, asList(promises));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Stream<? extends Promise<? extends T>> promises) {
		List<Promise<? extends T>> list = promises.collect(Collectors.toList());
		return toArray(type, list);
	}

	/**
	 * Reduces promises into Promise&lt;Array&gt;
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return toArray(type);
		if (size == 1) return toArray(type, promises.get(0));
		if (size == 2) return toArray(type, promises.get(0), promises.get(1));

		@SuppressWarnings("unchecked") PromiseToArray<T> resultPromise = new PromiseToArray<>((T[]) Array.newInstance(type, size));

		for (int i = 0; i < size; i++) {
			Promise<? extends T> promise = promises.get(i);
			if (promise.isResult()) {
				resultPromise.accumulator[i] = promise.materialize().getResult();
				continue;
			}
			if (promise.isException()) return Promise.ofException(promise.materialize().getException());
			int index = i;
			resultPromise.countdown++;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					resultPromise.processComplete(result, index);
				} else {
					resultPromise.tryCompleteExceptionally(e);
				}
			});
		}
		return resultPromise.countdown != 0 ? resultPromise : Promise.of(resultPromise.accumulator);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, R> Promise<R> toTuple(@NotNull TupleConstructor1<T1, R> constructor, @NotNull Promise<? extends T1> promise1) {
		return promise1.thenApply(constructor::create);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, R> Promise<R> toTuple(@NotNull TupleConstructor2<T1, T2, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2) {
		return promise1.combine(promise2, constructor::create);
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, R> Promise<R> toTuple(@NotNull TupleConstructor3<T1, T2, T3, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, R> Promise<R> toTuple(@NotNull TupleConstructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5, R> Promise<R> toTuple(@NotNull TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5, T6, R> Promise<R> toTuple(@NotNull TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5,
			@NotNull Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1> Promise<Tuple1<T1>> toTuple(@NotNull Promise<? extends T1> promise1) {
		return promise1.thenApply((Function<T1, Tuple1<T1>>) Tuple1::new);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2> Promise<Tuple2<T1, T2>> toTuple(@NotNull Promise<? extends T1> promise1, @NotNull Promise<? extends T2> promise2) {
		return promise1.combine(promise2, (BiFunction<T1, T2, Tuple2<T1, T2>>) Tuple2::new);
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3> Promise<Tuple3<T1, T2, T3>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.thenApply(list -> new Tuple3<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4> Promise<Tuple4<T1, T2, T3, T4>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.thenApply(list -> new Tuple4<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5> Promise<Tuple5<T1, T2, T3, T4, T5>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.thenApply(list -> new Tuple5<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5, T6> Promise<Tuple6<T1, T2, T3, T4, T5, T6>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5,
			@NotNull Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.thenApply(list -> new Tuple6<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	/**
	 * Returns a {@link CompleteNullPromise}
	 */
	@NotNull
	public static Promise<Void> sequence() {
		return Promise.complete();
	}

	/**
	 * Gets {@code Promise} from provided {@code AsyncSupplier},
	 * waits until it completes and than returns a {@code Promise<Void>}
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull AsyncSupplier<Void> promise) {
		return promise.get().toVoid();
	}

	/**
	 * Gets {@code Promise}s from provided {@code AsyncSupplier}s,
	 * end executes them consequently, discarding their results.
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull AsyncSupplier<Void> promise1, @NotNull AsyncSupplier<Void> promise2) {
		return promise1.get().thenCompose($ -> sequence(promise2));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	@NotNull
	@SafeVarargs
	public static Promise<Void> sequence(@NotNull AsyncSupplier<Void>... promises) {
		return sequence(asList(promises));
	}

	/**
	 * @see Promises#sequence(AsyncSupplier, AsyncSupplier)
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull AsyncSupplier<Void> promise1, @NotNull AsyncSupplier<Void> promise2, @NotNull AsyncSupplier<Void> promise3) {
		return promise1.get().thenCompose($ -> sequence(promise2, promise3));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull Iterable<? extends AsyncSupplier<Void>> promises) {
		return sequence(asPromises(promises.iterator()));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull Stream<? extends AsyncSupplier<Void>> promises) {
		return sequence(asPromises(promises.iterator()));
	}

	/**
	 * Calls every {@code Promise} from {@code promises} in sequence and discards
	 * their results.Returns a {@code SettablePromise} with {@code null} result as
	 * a marker when all of the {@code promises} are completed.
	 *
	 * @return {@code Promise} that completes when all {@code promises} are completed
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull Iterator<? extends Promise<Void>> promises) {
		SettablePromise<Void> cb = new SettablePromise<>();
		sequenceImpl(promises, cb);
		return cb;
	}

	private static void sequenceImpl(@NotNull Iterator<? extends Promise<Void>> promises, @NotNull SettablePromise<Void> cb) {
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) continue;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					sequenceImpl(promises, cb);
				} else {
					cb.setException(e);
				}
			});
			return;
		}
		cb.set(null);
	}

	/**
	 * Picks the first {@code Promise} that was completed without exception.
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	@SafeVarargs
	public static <T> Promise<T> firstSuccessful(AsyncSupplier<? extends T>... promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #firstSuccessful(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> firstSuccessful(@NotNull Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #firstSuccessful(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> firstSuccessful(@NotNull Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #firstSuccessful(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> firstSuccessful(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	@SafeVarargs
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull AsyncSupplier<? extends T>... promises) {
		return first(predicate, asList(promises));
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, asPromises(promises));
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, asPromises(promises));
	}

	/**
	 * @param predicate filters results, consumes result of {@code Promise}
	 * @return first completed result of {@code Promise} that satisfies predicate
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull Iterator<? extends Promise<? extends T>> promises) {
		SettablePromise<T> cb = new SettablePromise<>();
		firstImpl(promises, predicate, cb);
		return cb;
	}

	private static <T> void firstImpl(Iterator<? extends Promise<? extends T>> promises,
			@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull SettablePromise<T> cb) {
		if (!promises.hasNext()) {
			cb.setException(new StacklessException(Promises.class, "No promise result met the condition"));
			return;
		}
		promises.next().whenComplete((result, e) -> {
			if (predicate.test(result, e)) {
				cb.set(result, e);
				return;
			}
			firstImpl(promises, predicate, cb);
		});
	}

	/**
	 * Returns a {@link BiPredicate} which checks if
	 * {@code Promise} wasn't completed exceptionally.
	 */
	@Contract(value = " -> new", pure = true)
	@NotNull
	public static <T> BiPredicate<T, Throwable> isResult() {
		return ($, e) -> e == null;
	}

	/**
	 * Returns a {@link BiPredicate} which checks if
	 * {@code Promise} was completed with an exception.
	 */
	@Contract(value = " -> new", pure = true)
	@NotNull
	public static <T> BiPredicate<T, Throwable> isError() {
		return ($, e) -> e != null;
	}

	/**
	 * Repeats the operations of provided {@code supplier} infinitely,
	 * until one of the {@code Promise}s completes exceptionally.
	 */
	@NotNull
	public static Promise<Void> repeat(@NotNull Supplier<Promise<Void>> supplier) {
		SettablePromise<Void> cb = new SettablePromise<>();
		repeatImpl(supplier, cb);
		return cb;
	}

	private static void repeatImpl(@NotNull Supplier<Promise<Void>> supplier, @NotNull SettablePromise<Void> cb) {
		while (true) {
			Promise<Void> promise = supplier.get();
			if (!promise.isResult()) {
				promise.whenComplete(($, e) -> {
					if (e == null) {
						repeatImpl(supplier, cb);
					} else {
						cb.setException(e);
					}
				});
				return;
			}
		}
	}

	/**
	 * Repeats provided {@link Function} until can pass {@link Predicate} test.
	 * Resembles a simple Java {@code for()} loop but with async capabilities.
	 *
	 * @param seed start value
	 * @param test a boolean function which checks if this loop can continue
	 * @param next a function applied to the seed, returns {@code Promise}
	 * @return {@link SettablePromise} with {@code null} result if it was
	 * completed successfully, otherwise returns a {@code SettablePromise}
	 * with an exception. In both situations returned {@code Promise}
	 * is a marker of completion of the loop.
	 */
	@NotNull
	public static <T> Promise<Void> loop(@Nullable T seed, @NotNull Predicate<T> test, @NotNull Function<T, Promise<T>> next) {
		if (!test.test(seed)) return Promise.complete();
		SettablePromise<Void> cb = new SettablePromise<>();
		loopImpl(seed, test, next, cb);
		return cb;
	}

	/**
	 * @see #loop(Object, Predicate, Function)
	 */
	@NotNull
	public static Promise<Void> loop(@NotNull Predicate<Void> test, @NotNull Function<Void, Promise<Void>> next) {
		return loop(null, test, next);
	}

	private static <T> void loopImpl(@Nullable T seed, @NotNull Predicate<T> test, @NotNull Function<T, Promise<T>> next, @NotNull SettablePromise<Void> cb) {
		while (true) {
			Promise<T> promise = next.apply(seed);
			if (promise.isResult()) {
				seed = promise.materialize().getResult();
				if (!test.test(seed)) {
					cb.set(null);
					break;
				}
			} else {
				promise.whenComplete((newSeed, e) -> {
					if (e == null) {
						if (test.test(newSeed)) {
							loopImpl(newSeed, test, next, cb);
						} else {
							cb.set(null);
						}
					} else {
						cb.setException(e);
					}
				});
				return;
			}
		}
	}

	/**
	 * Transforms a collection of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	@SuppressWarnings("unchecked")
	@NotNull
	public static <T> Iterator<Promise<T>> asPromises(@NotNull Iterator<? extends AsyncSupplier<? extends T>> tasks) {
		return transformIterator((Iterator<AsyncSupplier<T>>) tasks, AsyncSupplier::get);
	}

	/**
	 * Transforms a {@link Stream} of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	public static <T> Iterator<Promise<T>> asPromises(@NotNull Stream<? extends AsyncSupplier<? extends T>> tasks) {
		return asPromises(tasks.iterator());
	}

	/**
	 * Transforms an {@link Iterable} of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	public static <T> Iterator<Promise<T>> asPromises(@NotNull Iterable<? extends AsyncSupplier<? extends T>> tasks) {
		return asPromises(tasks.iterator());
	}

	/**
	 * Transforms an {@link AsyncSupplier} {@code tasks}
	 * to a collection of {@code Promise}s.
	 */
	@SafeVarargs
	public static <T> Iterator<Promise<T>> asPromises(@NotNull AsyncSupplier<? extends T>... tasks) {
		return asPromises(asList(tasks));
	}

	/**
	 * Allows to asynchronously reduce {@link Iterator} of {@code Promise}s
	 * into a {@code Promise} with the help of {@link Collector}. You can
	 * control the amount of concurrently running {@code Promise}.
	 * <p>
	 * This method is universal and allows to implement app-specific logic.
	 *
	 * @param collector mutable reduction operation that accumulates input
	 *                     elements into a mutable result container
	 * @param maxCalls 	max amount of concurrently running {@code Promise}s
	 * @param promises	{@code Iterable} of {@code Promise}s
	 * @param <T>		type of input elements for this operation
	 * @param <A>		mutable accumulation type of the operation
	 * @param <R>		the result type of the operation
	 * @return			a {@code Promise} which wraps the accumulated result
	 * of the reduction. If one of the {@code promises} completed exceptionally,
	 * a {@code Promise} with an exception will be returned.
	 */
	public static <T, A, R> Promise<R> reduce(@NotNull Collector<T, A, R> collector, int maxCalls,
			@NotNull Iterator<Promise<T>> promises) {
		return reduce(promises, maxCalls, collector.supplier().get(), collector.accumulator(), collector.finisher());
	}

	/**
	 * @param promises 	  {@code Iterable} of {@code Promise}s
	 * @param accumulator supplier of the result
	 * @param maxCalls	  max amount of concurrently running {@code Promise}s
	 * @param consumer	  a {@link BiConsumer} which folds a result of each of the
	 *                       completed {@code promises} into accumulator
	 * @param finisher	  a {@link Function} which performs the final transformation
	 *                       from the intermediate accumulations
	 * @param <T>		  type of input elements for this operation
	 * @param <A>		  mutable accumulation type of the operation
	 * @param <R>		  result type of the reduction operation
	 * @return			  a {@code Promise} which wraps the accumulated result of the
	 * reduction. If one of the {@code promises} completed exceptionally, a {@code Promise}
	 * with an exception will be returned.
	 *
	 * @see #reduce(Collector, int, Iterator)
	 */
	public static <T, A, R> Promise<R> reduce(@NotNull Iterator<Promise<T>> promises, int maxCalls,
			A accumulator,
			@NotNull BiConsumer<A, T> consumer,
			@NotNull Function<A, R> finisher) {
		return Promise.ofCallback(cb ->
				reduceImpl(promises, maxCalls, new int[]{0},
						accumulator, consumer, finisher, cb));
	}

	private static <T, A, R> void reduceImpl(Iterator<Promise<T>> promises, int maxCalls, int[] calls,
			A accumulator, BiConsumer<A, T> consumer, Function<A, R> finisher,
			SettablePromise<R> cb) {
		while (promises.hasNext() && calls[0] < maxCalls) {
			assert !cb.isComplete();
			Promise<T> promise = promises.next();
			if (promise.isComplete()) {
				if (promise.isResult()) {
					consumer.accept(accumulator, promise.materialize().getResult());
					continue;
				} else {
					cb.setException(promise.materialize().getException());
					return;
				}
			}
			calls[0]++;
			promise.whenComplete((v, e) -> {
				calls[0]--;
				if (cb.isComplete()) {
					return;
				}
				if (e == null) {
					consumer.accept(accumulator, v);
					reduceImpl(promises, maxCalls, calls,
							accumulator, consumer, finisher, cb);
				} else {
					cb.setException(e);
				}
			});
		}
		if (calls[0] == 0) {
			R result = finisher.apply(accumulator);
			cb.set(result);
		}
	}

	/**
	 * Allows to asynchronously reduce {@link Iterator} of {@code Promise}s
	 * into a {@code Promise} with the help of {@link Collector}. You can
	 * control the amount of concurrently running {@code promises} and explicitly
	 * process exceptions and intermediate results.
	 * <p>
	 * The main feature of this method is that you can set up {@code consumer}
	 * for different use cases, for example:
	 * <ul>
	 *     <li> If one of the {@code promises} completes exceptionally, reduction
	 *     will stop without waiting for all of the {@code promises} to be completed.
	 *     A {@code Promise} with exception will be returned.
	 *     <li> If one of the {@code promises} finishes with needed result, reduction
	 *     will stop without waiting for all of the {@code promises} to be completed.
	 *     <li> If a needed result accumulates before all of the {@code promises} run,
	 *     reduction will stop without waiting for all of the {@code promises} to be completed.
	 * </ul>
	 * <p>
	 * To implement the use cases, you need to set up the provided {@code consumer}'s
	 * {@link BiFunction#apply(Object, Object)} function. This function will be applied
	 * to each of the completed {@code promises} and corresponding accumulated result.
	 * <p>
	 * When {@code apply} returns {@code null}, nothing happens and reduction continues.
	 * When {@link Try} with any result or exception is returned, the reduction stops without
	 * waiting for all of the {@code promises} to be completed and {@code Promise} with
	 * {@code Try}'s result or exception is returned
	 * .
	 *
	 * @param promises 		{@code Iterable} of {@code Promise}s
	 * @param maxCalls 		{@link ToIntFunction} which calculates max amount of concurrently
	 *                         running {@code Promise}s based on the {@code accumulator} value
	 * @param accumulator   mutable supplier of the result
	 * @param consumer 		a {@link BiConsumer} which folds a result of each of the completed
	 *                         {@code promises} into accumulator for further processing
	 * @param finisher      a {@link Function} which performs the final transformation
	 *                         from the intermediate accumulations
	 * @param recycler		processes results of those {@code promises} which were
	 *                         completed after result of the reduction was returned
	 * @param <T>		    type of input elements for this operation
	 * @param <A>		    mutable accumulation type of the operation
	 * @param <R>		    result type of the reduction operation
	 * @return				a {@code Promise} which wraps accumulated result or exception.
	 */
	public static <T, A, R> Promise<R> reduceEx(@NotNull Iterator<Promise<T>> promises, @NotNull ToIntFunction<A> maxCalls,
			A accumulator,
			@NotNull BiFunction<A, Try<T>, @Nullable Try<R>> consumer,
			@NotNull Function<A, @NotNull Try<R>> finisher,
			@Nullable Consumer<T> recycler) {
		return Promise.ofCallback(cb ->
				reduceExImpl(promises, maxCalls, new int[]{0},
						accumulator, consumer, finisher, recycler, cb));
	}

	private static <T, A, R> void reduceExImpl(Iterator<Promise<T>> promises, ToIntFunction<A> maxCalls, int[] calls,
			A accumulator, BiFunction<A, Try<T>, Try<R>> consumer, Function<A, @NotNull Try<R>> finisher, @Nullable Consumer<T> recycler,
			SettablePromise<R> cb) {
		while (promises.hasNext() && calls[0] < maxCalls.applyAsInt(accumulator)) {
			assert !cb.isComplete();
			Promise<T> promise = promises.next();
			if (promise.isComplete()) {
				@Nullable Try<R> maybeResult = consumer.apply(accumulator, promise.materialize().getTry());
				if (maybeResult != null) {
					cb.set(maybeResult.getOrNull(), maybeResult.getExceptionOrNull());
					return;
				}
				continue;
			}
			calls[0]++;
			promise.whenComplete((v, e) -> {
				calls[0]--;
				if (cb.isComplete()) {
					if (recycler != null) recycler.accept(v);
					return;
				}
				@Nullable Try<R> maybeResult = consumer.apply(accumulator, Try.of(v, e));
				if (maybeResult != null) {
					cb.set(maybeResult.getOrNull(), maybeResult.getExceptionOrNull());
				} else {
					reduceExImpl(promises, maxCalls, calls,
							accumulator, consumer, finisher, recycler, cb);
				}
			});
		}
		if (calls[0] == 0) {
			@NotNull Try<R> result = finisher.apply(accumulator);
			if (result.isSuccess()) {
				cb.set(result.get());
			} else {
				cb.setException(result.getException());
			}
		}
	}

	// region helper classes
	private static final class PromiseAll<T> extends NextPromise<T, Void> {
		int countdown;

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				if (--countdown == 0) {
					complete(null);
				}
			} else {
				tryCompleteExceptionally(e);
			}
		}
	}

	private static final class PromiseAny<T> extends NextPromise<T, T> {
		int errors;

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				tryComplete(result);
			} else {
				if (--errors == 0) {
					completeExceptionally(e);
				}
			}
		}
	}

	private static final class PromiseToArray<T> extends NextPromise<T, T[]> {
		T[] accumulator;
		int countdown;

		private PromiseToArray(@NotNull T[] accumulator) {
			this.accumulator = accumulator;
		}

		void processComplete(@Nullable T result, int i) {
			if (isComplete()) {
				return;
			}
			accumulator[i] = result;
			if (--countdown == 0) {
				T[] accumulator = this.accumulator;
				this.accumulator = null;
				complete(accumulator);
			}
		}

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				processComplete(result, 0);
			} else {
				tryCompleteExceptionally(e);
			}
		}
	}

	private static final class PromiseToList<T> extends NextPromise<T, List<T>> {
		T[] accumulator;
		int countdown;

		private PromiseToList(@NotNull T[] accumulator) {
			this.accumulator = accumulator;
		}

		void processComplete(@Nullable T result, int i) {
			if (isComplete()) {
				return;
			}
			accumulator[i] = result;
			if (--countdown == 0) {
				T[] accumulator = this.accumulator;
				this.accumulator = null;
				complete(Arrays.asList(accumulator));
			}
		}

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				processComplete(result, 0);
			} else {
				tryCompleteExceptionally(e);
			}
		}
	}
	// endregion
}
