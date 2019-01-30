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

	/**
	 * Returns one of the first completed {@code Promise}s. Since it's
	 * async, we can't really get the FIRST completed {@code Promise}.
	 *
	 * @return one of the first completed {@code Promise}s
	 */
	@NotNull
	public static <T> Promise<T> any(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		if (!promises.hasNext()) return any();
		@NotNull PromiseAny<T> resultPromise = new PromiseAny<>();
		while (promises.hasNext()) {
			Promise<? extends T> promise = promises.next();
			if (promise.isResult()) return Promise.of(promise.materialize().getResult());
			if (promise.isException()) continue;
			resultPromise.errors++;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					resultPromise.tryComplete(result);
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
	 * Accumulates results of {@code Promise}s using {@link IndexedCollector}.
	 *
	 * @param <T>       type of input value
	 * @param <A>       type of accumulator
	 * @param <R>       type of result
	 * @param collector reducer which is used for combining {@code Promise} results into one value
	 * @param promises  collection of {@code Promise}s
	 * @return {@code Promise} with accumulated result
	 */
	@Contract(pure = true)
	@NotNull
	public static <T, A, R> Promise<R> collect(@NotNull IndexedCollector<T, A, R> collector, @NotNull List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.of(collector.resultOf());
		if (size == 1) return promises.get(0).thenApply(collector::resultOf);
		if (size == 2) return promises.get(0).combine(promises.get(1), collector::resultOf);

		A accumulator = collector.accumulator(size);
		@NotNull PromiseIndexedCollect<T, A, R> resultPromise = new PromiseIndexedCollect<>(collector, accumulator);

		for (int i = 0; i < size; i++) {
			Promise<? extends T> promise = promises.get(i);
			if (promise.isResult()) {
				collector.accumulate(resultPromise.accumulator, i, promise.materialize().getResult());
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
		return resultPromise.countdown != 0 ? resultPromise : Promise.of(collector.finish(resultPromise.accumulator));
	}

	@NotNull
	public static <T, A, R> Promise<R> collect(@NotNull Collector<T, A, R> collector, @NotNull Iterator<? extends Promise<? extends T>> promises) {
		A accumulatorValue = collector.supplier().get();
		BiConsumer<A, T> accumulatorConsumer = collector.accumulator();
		if (!promises.hasNext()) {
			return Promise.of(collector.finisher().apply(accumulatorValue));
		}
		@NotNull PromiseCollect<T, A, R> resultPromise = new PromiseCollect<>(collector, accumulatorValue);

		while (promises.hasNext()) {
			Promise<? extends T> promise = promises.next();
			if (promise.isResult()) {
				accumulatorConsumer.accept(resultPromise.accumulator, promise.materialize().getResult());
				continue;
			}
			if (promise.isException()) return Promise.ofException(promise.materialize().getException());
			resultPromise.countdown++;
			promise.then(resultPromise);
		}
		return resultPromise.countdown != 0 ? resultPromise : Promise.of(collector.finisher().apply(resultPromise.accumulator));
	}

	/**
	 * @see #collect(Collector, Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T, A, R> Promise<R> collect(@NotNull Collector<T, A, R> collector, @NotNull Iterable<? extends Promise<? extends T>> promises) {
		return collect(collector, promises.iterator());
	}

	/**
	 * @see #collect(Collector, Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T, A, R> Promise<R> collect(@NotNull Collector<T, A, R> collector, @NotNull Stream<? extends Promise<? extends T>> promises) {
		return collect(collector, promises.iterator());
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
	 *
	 * @see Promises#collect(IndexedCollector, List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull List<? extends Promise<? extends T>> promises) {
		return collect(IndexedCollector.toList(), promises);
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
	 *
	 * @see Promises#collect(IndexedCollector, List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull List<? extends Promise<? extends T>> promises) {
		return collect(IndexedCollector.toArray(type), promises);
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
	public static Promise<Void> runSequence() {
		return Promise.complete();
	}

	/**
	 * Gets {@code Promise} from provided {@code AsyncSupplier},
	 * waits until it completes and than returns a {@code Promise<Void>}
	 */
	@NotNull
	public static Promise<Void> runSequence(@NotNull AsyncSupplier<?> promise) {
		return promise.get().toVoid();
	}

	/**
	 * Gets {@code Promise}s from provided {@code AsyncSupplier}s,
	 * end executes them consequently, discarding their results.
	 */
	@NotNull
	public static Promise<Void> runSequence(@NotNull AsyncSupplier<?> promise1, @NotNull AsyncSupplier<?> promise2) {
		return promise1.get().thenCompose($ -> runSequence(promise2));
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	@NotNull
	public static Promise<Void> runSequence(@NotNull AsyncSupplier<?>... promises) {
		return runSequence(asList(promises));
	}

	/**
	 * @see Promises#runSequence(AsyncSupplier, AsyncSupplier)
	 */
	@NotNull
	public static Promise<Void> runSequence(@NotNull AsyncSupplier<?> promise1, @NotNull AsyncSupplier<?> promise2, @NotNull AsyncSupplier<?> promise3) {
		return promise1.get().thenCompose($ -> runSequence(promise2, promise3));
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	@NotNull
	public static Promise<Void> runSequence(@NotNull Iterable<? extends AsyncSupplier<?>> promises) {
		return runSequence(asPromises(promises.iterator()));
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	@NotNull
	public static Promise<Void> runSequence(@NotNull Stream<? extends AsyncSupplier<?>> promises) {
		return runSequence(asPromises(promises.iterator()));
	}

	/**
	 * Calls every {@code Promise} from {@code promises} in sequence and discards
	 * their results.Returns a {@code SettablePromise} with {@code null} result as
	 * a marker when all of the {@code promises} are completed.
	 *
	 * @return {@code Promise} that completes when all {@code promises} are completed
	 */
	@NotNull
	public static Promise<Void> runSequence(@NotNull Iterator<? extends Promise<?>> promises) {
		SettablePromise<Void> result = new SettablePromise<>();
		runSequenceImpl(promises, result);
		return result;
	}

	private static void runSequenceImpl(@NotNull Iterator<? extends Promise<?>> promises, @NotNull SettablePromise<Void> cb) {
		if (!promises.hasNext()) {
			cb.set(null);
			return;
		}
		promises.next().whenComplete((result, e) -> {
			if (e == null) {
				runSequenceImpl(promises, cb);
			} else {
				cb.setException(e);
			}
		});
	}

	/**
	 * @see Promises#collectSequence(Collector, Iterator)
	 */
	@NotNull
	@SafeVarargs
	public static <T, A, R> Promise<R> collectSequence(@NotNull Collector<T, A, R> collector, @NotNull AsyncSupplier<? extends T>... promises) {
		return collectSequence(collector, asList(promises));
	}

	/**
	 * @see Promises#collectSequence(Collector, Iterator)
	 */
	@NotNull
	public static <T, A, R> Promise<R> collectSequence(@NotNull Collector<T, A, R> collector, @NotNull Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return collectSequence(collector, asPromises(promises.iterator()));
	}

	/**
	 * @see Promises#collectSequence(Collector, Iterator)
	 */
	@NotNull
	public static <T, A, R> Promise<R> collectSequence(@NotNull Collector<T, A, R> collector, @NotNull Stream<? extends AsyncSupplier<? extends T>> promises) {
		return collectSequence(collector, asPromises(promises.iterator()));
	}

	/**
	 * Accumulates {@code promises} results into one final using {@link Collector} sequentially.
	 *
	 * @return new {@code Promise} that completes when all {@code promises} are completed
	 */
	@NotNull
	public static <T, A, R> Promise<R> collectSequence(@NotNull Collector<T, A, R> collector, @NotNull Iterator<? extends Promise<? extends T>> promises) {
		@NotNull SettablePromise<R> result = new SettablePromise<>();
		collectSequenceImpl(promises, collector.accumulator(), collector.finisher(), collector.supplier().get(), result);
		return result;
	}

	private static <T, A, R> void collectSequenceImpl(@NotNull Iterator<? extends Promise<? extends T>> promises,
			@NotNull BiConsumer<A, T> accumulator, @NotNull Function<A, R> finisher,
			@Nullable A accumulatedValue, @NotNull SettablePromise<R> cb) {
		if (!promises.hasNext()) {
			cb.set(finisher.apply(accumulatedValue));
			return;
		}
		promises.next().whenComplete((result, e) -> {
			if (e == null) {
				accumulator.accept(accumulatedValue, result);
				collectSequenceImpl(promises, accumulator, finisher, accumulatedValue, cb);
			} else {
				cb.setException(e);
			}
		});
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
		return first(predicate, asPromises(promises.iterator()));
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, asPromises(promises.iterator()));
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

	public static <T> Iterator<Promise<T>> asPromises(@NotNull Stream<? extends AsyncSupplier<? extends T>> tasks) {
		return asPromises(tasks.iterator());
	}

	public static <T, A, R> Promise<R> reduce(@NotNull Iterator<Promise<T>> promises,
			@NotNull Supplier<A> accumulatorSupplier,
			int maxCalls,
			@NotNull BiConsumer<A, T> consumer,
			@NotNull Function<A, R> finalizer,
			@Nullable Consumer<A> recycler) {
		return Promise.ofCallback(cb ->
				reduceImpl(promises, maxCalls, new int[]{0},
						accumulatorSupplier.get(), consumer, finalizer, recycler, cb));
	}

	private static <T, A, R> void reduceImpl(Iterator<Promise<T>> promises, int maxCalls, int[] calls,
			A accumulator, BiConsumer<A, T> consumer, Function<A, R> finalizer, @Nullable Consumer<A> recycler,
			SettablePromise<R> cb) {
		while (promises.hasNext() && calls[0] < maxCalls) {
			assert !cb.isComplete();
			Promise<T> promise = promises.next();
			if (promise.isComplete()) {
				if (promise.isResult()) {
					consumer.accept(accumulator, promise.materialize().getResult());
					continue;
				} else {
					if (recycler != null) recycler.accept(accumulator);
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
							accumulator, consumer, finalizer, recycler, cb);
				} else {
					if (recycler != null) recycler.accept(accumulator);
					cb.setException(e);
				}
			});
		}
		if (calls[0] == 0) {
			R result = finalizer.apply(accumulator);
			cb.set(result);
		}
	}

	public static <T, A, R> Promise<R> reduceEx(@NotNull Iterator<Promise<T>> promises,
			@NotNull Supplier<A> accumulatorSupplier,
			@NotNull ToIntFunction<A> maxCalls,
			@NotNull BiFunction<A, Try<T>, @Nullable Try<R>> consumer,
			@NotNull Function<A, @NotNull Try<R>> finalizer,
			@Nullable Consumer<T> recycler) {
		return Promise.ofCallback(cb ->
				reduceExImpl(promises, maxCalls, new int[]{0},
						accumulatorSupplier.get(), consumer, finalizer, recycler, cb));
	}

	private static <T, A, R> void reduceExImpl(Iterator<Promise<T>> promises, ToIntFunction<A> maxCalls, int[] calls,
			A accumulator, BiFunction<A, Try<T>, Try<R>> consumer, Function<A, @NotNull Try<R>> finalizer, @Nullable Consumer<T> recycler,
			SettablePromise<R> cb) {
		while (promises.hasNext() && calls[0] < maxCalls.applyAsInt(accumulator)) {
			assert !cb.isComplete();
			Promise<T> promise = promises.next();
			if (promise.isComplete()) {
				@Nullable Try<R> maybeResult = consumer.apply(accumulator, promise.materialize().getTry());
				if (maybeResult != null) {
					if (maybeResult.isSuccess()) {
						cb.set(maybeResult.get());
					} else {
						cb.setException(maybeResult.getException());
					}
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
					if (maybeResult.isSuccess()) {
						cb.set(maybeResult.get());
					} else {
						cb.setException(maybeResult.getException());
					}
				} else {
					reduceExImpl(promises, maxCalls, calls,
							accumulator, consumer, finalizer, recycler, cb);
				}
			});
		}
		if (calls[0] == 0) {
			@NotNull Try<R> result = finalizer.apply(accumulator);
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

	private static final class PromiseCollect<T, A, R> extends NextPromise<T, R> {
		@NotNull
		final Collector<T, A, R> collector;
		@Nullable
		A accumulator;
		int countdown;

		private PromiseCollect(@NotNull Collector<T, A, R> collector, @Nullable A accumulator) {
			this.collector = collector;
			this.accumulator = accumulator;
		}

		void processComplete(@Nullable T result) {
			if (isComplete()) {
				return;
			}
			collector.accumulator().accept(accumulator, result);
			if (--countdown == 0) {
				R reducerResult = collector.finisher().apply(accumulator);
				//noinspection AssignmentToNull - resource release
				accumulator = null;
				complete(reducerResult);
			}
		}

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				processComplete(result);
			} else {
				tryCompleteExceptionally(e);
			}
		}
	}

	private static final class PromiseIndexedCollect<T, A, R> extends NextPromise<T, R> {
		@NotNull
		final IndexedCollector<T, A, R> collector;
		@Nullable
		A accumulator;
		int countdown;

		private PromiseIndexedCollect(@NotNull IndexedCollector<T, A, R> collector, @Nullable A accumulator) {
			this.collector = collector;
			this.accumulator = accumulator;
		}

		private PromiseIndexedCollect(@NotNull Collector<T, A, R> collector, @Nullable A accumulator) {
			this.collector = IndexedCollector.ofCollector(collector);
			this.accumulator = accumulator;
		}

		void processComplete(@Nullable T result, int i) {
			if (isComplete()) {
				return;
			}
			collector.accumulate(accumulator, i, result);
			if (--countdown == 0) {
				R reducerResult = collector.finish(accumulator);
				//noinspection AssignmentToNull - resource release
				accumulator = null;
				complete(reducerResult);
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

	static final class ReduceTimeouter<T, A, R> implements Runnable, CollectListener<T, A, R> {
		CollectCanceller canceller;
		@Nullable
		ScheduledRunnable scheduledRunnable;

		@Override
		public void onStart(@NotNull CollectCanceller canceller, @Nullable A accumulator) {
			this.canceller = canceller;
		}

		@Override
		public void onCollectResult(@Nullable R result) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
			}
		}

		@Override
		public void onCollectException(@NotNull Throwable e) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
			}
		}

		@Override
		public void run() {
			canceller.finish();
		}
	}
	// endregion
}
