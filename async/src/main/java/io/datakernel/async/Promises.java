/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.util.*;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.CollectionUtils.transform;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public final class Promises {
	private Promises() {}

	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException(Promises.class, "Promise timeout");

	public static <T> Promise<T> timeout(Promise<T> promise, long delay) {
		if (promise.isComplete()) return promise;
		return promise.then(new NextPromise<T, T>() {
			ScheduledRunnable schedule = getCurrentEventloop().delay(delay, () -> tryCompleteExceptionally(TIMEOUT_EXCEPTION));

			@Override
			public void accept(T result, Throwable e) {
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

	public static <T> Promise<T> timeout(Promise<T> promise, Duration delay) {
		return timeout(promise, delay.toMillis());
	}

	/**
	 * @see Promises#all(List)
	 */
	public static Promise<Void> all() {
		return Promise.complete();
	}

	/**
	 * @see Promises#all(List)
	 */
	public static Promise<Void> all(Promise<?> promise1) {
		return promise1.toVoid();
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see Promises#all(List)
	 */
	public static Promise<Void> all(Promise<?> promise1, Promise<?> promise2) {
		return promise1.both(promise2);
	}

	/**
	 * @see Promises#all(List)
	 */
	public static Promise<Void> all(Promise<?>... promises) {
		return all(asIterator(promises));
	}

	/**
	 * @see Promises#all(List)
	 */
	public static Promise<Void> all(Stream<? extends Promise<?>> promises) {
		return all(promises.iterator());
	}

	/**
	 * @see Promises#all(List)
	 */
	public static Promise<Void> all(Iterable<? extends Promise<?>> promises) {
		return all(promises.iterator());
	}

	/**
	 * @return {@code Promise} that completes when all promises are completed
	 */
	public static Promise<Void> all(List<? extends Promise<?>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.complete();
		if (size == 1) return promises.get(0).toVoid();
		if (size == 2) return promises.get(0).both(promises.get(1));
		return all(promises.iterator());
	}

	/**
	 * @return {@code Promise} that completes when all promises are completed
	 */
	public static Promise<Void> all(Iterator<? extends Promise<?>> promises) {
		if (!promises.hasNext()) return all();
		PromiseAll<Object> resultPromise = new PromiseAll<>();
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) continue;
			if (promise.isException()) return Promise.ofException(promise.materialize().getException());
			resultPromise.countdown++;
			promise.then(resultPromise);
		}
		return resultPromise.countdown != 0 ? resultPromise : Promise.complete();
	}

	public static <T> Promise<T> any() {
		return Promise.ofException(new NoSuchElementException());
	}

	/**
	 * @see Promises#any(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(Promise<? extends T> promise1) {
		return (Promise<T>) promise1;
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see Promises#any(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(Promise<? extends T> promise1, Promise<? extends T> promise2) {
		return ((Promise<T>) promise1).either(promise2);
	}

	/**
	 * @see Promises#any(List)
	 */
	@SafeVarargs
	public static <T> Promise<T> any(Promise<? extends T>... promises) {
		return any(asIterator(promises));
	}

	public static <T> Promise<T> any(Stream<? extends Promise<? extends T>> promises) {
		return any(promises.iterator());
	}

	public static <T> Promise<T> any(Iterable<? extends Promise<? extends T>> promises) {
		return any(promises.iterator());
	}

	/**
	 * @return first completed promise
	 * this method returns one of the first completed promises, because it's async we can't really get FIRST completed promise.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 1) return (Promise<T>) promises.get(0);
		if (size == 2) return ((Promise<T>) promises.get(0)).either(promises.get(1));
		return any(promises.iterator());
	}

	public static <T> Promise<T> any(Iterator<? extends Promise<? extends T>> promises) {
		if (!promises.hasNext()) return any();
		PromiseAny<T> resultPromise = new PromiseAny<>();
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
	 * Accumulates results of {@code Promise}s using {@code IndexedCollector}.
	 *
	 * @param <T>       type of input value
	 * @param <A>       type of accumulator
	 * @param <R>       type of result
	 * @param collector reducer which is used for combining {@code Promise} results into one value
	 * @param promises    collection of {@code Promise}s
	 * @return {@code Promise} with accumulated result
	 * @see IndexedCollector
	 */
	public static <T, A, R> Promise<R> collect(IndexedCollector<T, A, R> collector, List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.of(collector.resultOf());
		if (size == 1) return promises.get(0).thenApply(collector::resultOf);
		if (size == 2) return promises.get(0).combine(promises.get(1), collector::resultOf);

		A accumulator = collector.accumulator(size);
		PromiseIndexedCollect<T, A, R> resultPromise = new PromiseIndexedCollect<>(collector, accumulator);

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

	/**
	 * Allows you to do something on completion of every {@code Promise}.
	 *
	 * @param listener calls {@link CollectListener#onCollectResult(Object)} with every {@code Promise} result
	 * @see Promises#collect(IndexedCollector, List)
	 */
	public static <T, A, R> Promise<R> collect(IndexedCollector<T, A, R> collector, CollectListener<T, A, R> listener, List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) {
			R finished = collector.resultOf();
			listener.onCollectResult(finished);
			return Promise.of(finished);
		}

		A accumulator = collector.accumulator(size);
		PromiseCollectEx<T, A, R> resultPromise = new PromiseCollectEx<>(collector, listener, accumulator);
		listener.onStart(resultPromise, accumulator);

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
					resultPromise.processException(e, index);
				}
			});
		}
		if (resultPromise.countdown == 0) {
			R result = collector.finish(resultPromise.accumulator);
			listener.onCollectResult(result);
			return Promise.of(result);
		} else {
			return resultPromise;
		}
	}

	public static <T, A, R> Promise<R> collect(Collector<T, A, R> collector, Iterator<? extends Promise<? extends T>> promises) {
		A accumulatorValue = collector.supplier().get();
		BiConsumer<A, T> accumulatorConsumer = collector.accumulator();
		if (!promises.hasNext()) {
			return Promise.of(collector.finisher().apply(accumulatorValue));
		}
		PromiseCollect<T, A, R> resultPromise = new PromiseCollect<>(collector, accumulatorValue);

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

	public static <T, A, R> Promise<R> collect(Collector<T, A, R> collector, Iterable<? extends Promise<? extends T>> promises) {
		return collect(collector, promises.iterator());
	}

	public static <T, A, R> Promise<R> collect(Collector<T, A, R> collector, Stream<? extends Promise<? extends T>> promises) {
		return collect(collector, promises.iterator());
	}

	public static <T> Promise<List<T>> toList() {
		return Promise.of(emptyList());
	}

	/**
	 * @see Promises#toList(Promise[])
	 */
	public static <T> Promise<List<T>> toList(Promise<? extends T> promise1) {
		return promise1.thenApply(Arrays::asList);
	}

	/**
	 * @see Promises#toList(Promise[])
	 */
	public static <T> Promise<List<T>> toList(Promise<? extends T> promise1, Promise<? extends T> promise2) {
		return promise1.combine(promise2, (value1, value2) -> asList(value1, value2));
	}

	/**
	 * @see Promises#toList(Promise[])
	 */
	@SafeVarargs
	public static <T> Promise<List<T>> toList(Promise<? extends T>... promises) {
		return toList(asList(promises));
	}

	/**
	 * @see Promises#toList(Promise[])
	 */
	public static <T> Promise<List<T>> toList(Stream<? extends Promise<? extends T>> promises) {
		List<Promise<? extends T>> list = promises.collect(Collectors.toList());
		return toList(list);
	}

	/**
	 * Prepared version of reduce that reduces promises into Promise&lt;List&gt;
	 *
	 * @see Promises#collect(IndexedCollector, List)
	 */
	public static <T> Promise<List<T>> toList(List<? extends Promise<? extends T>> promises) {
		return collect(IndexedCollector.toList(), promises);
	}

	@SuppressWarnings("unchecked")
	public static <T> Promise<T[]> toArray(Class<T> type) {
		return Promise.of((T[]) Array.newInstance(type, 0));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T[]> toArray(Class<T> type, Promise<? extends T> promise1) {
		return promise1.thenApply(value -> {
			T[] array = (T[]) Array.newInstance(type, 1);
			array[0] = value;
			return array;
		});
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T[]> toArray(Class<T> type, Promise<? extends T> promise1, Promise<? extends T> promise2) {
		return promise1.combine(promise2, (value1, value2) -> {
			T[] array = (T[]) Array.newInstance(type, 2);
			array[0] = value1;
			array[1] = value2;
			return array;
		});
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@SafeVarargs
	public static <T> Promise<T[]> toArray(Class<T> type, Promise<? extends T>... promises) {
		return toArray(type, asList(promises));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	public static <T> Promise<T[]> toArray(Class<T> type, Stream<? extends Promise<? extends T>> promises) {
		List<Promise<? extends T>> list = promises.collect(Collectors.toList());
		return toArray(type, list);
	}

	/**
	 * Prepared version of reduce that reduces promises into Promise&lt;Array&gt;
	 *
	 * @see Promises#collect(IndexedCollector, List)
	 */
	public static <T> Promise<T[]> toArray(Class<T> type, List<? extends Promise<? extends T>> promises) {
		return collect(IndexedCollector.toArray(type), promises);
	}

	public static <T1, R> Promise<R> toTuple(TupleConstructor1<T1, R> constructor, Promise<? extends T1> promise1) {
		return promise1.thenApply(constructor::create);
	}

	public static <T1, T2, R> Promise<R> toTuple(TupleConstructor2<T1, T2, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2) {
		return promise1.combine(promise2, constructor::create);
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, R> Promise<R> toTuple(TupleConstructor3<T1, T2, T3, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, R> Promise<R> toTuple(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, R> Promise<R> toTuple(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, R> Promise<R> toTuple(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5,
			Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	public static <T1> Promise<Tuple1<T1>> toTuple(Promise<? extends T1> promise1) {
		return promise1.thenApply((Function<T1, Tuple1<T1>>) Tuple1::new);
	}

	public static <T1, T2> Promise<Tuple2<T1, T2>> toTuple(Promise<? extends T1> promise1, Promise<? extends T2> promise2) {
		return promise1.combine(promise2, (BiFunction<T1, T2, Tuple2<T1, T2>>) Tuple2::new);
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Promise<Tuple3<T1, T2, T3>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.thenApply(list -> new Tuple3<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Promise<Tuple4<T1, T2, T3, T4>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.thenApply(list -> new Tuple4<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Promise<Tuple5<T1, T2, T3, T4, T5>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.thenApply(list -> new Tuple5<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Promise<Tuple6<T1, T2, T3, T4, T5, T6>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5,
			Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.thenApply(list -> new Tuple6<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	public static Promise<Void> runSequence() {
		return Promise.complete();
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	public static Promise<Void> runSequence(AsyncSupplier<?> promise) {
		return promise.get().toVoid();
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	public static Promise<Void> runSequence(AsyncSupplier<?> promise1, AsyncSupplier<?> promise2) {
		return promise1.get().thenCompose($ -> runSequence(promise2));
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	public static Promise<Void> runSequence(AsyncSupplier<?>... promises) {
		return runSequence(asList(promises));
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	public static Promise<Void> runSequence(AsyncSupplier<?> promise1, AsyncSupplier<?> promise2, AsyncSupplier<?> promise3) {
		return promise1.get().thenCompose($ -> runSequence(promise2, promise3));
	}

	/**
	 * @see Promises#runSequence(Iterator)
	 */
	public static Promise<Void> runSequence(Iterable<? extends AsyncSupplier<?>> promises) {
		return runSequence(transform(promises.iterator(), AsyncSupplier::get));
	}

	public static Promise<Void> runSequence(Stream<? extends Promise<?>> promises) {
		return runSequence(promises.iterator());
	}

	/**
	 * Calls every {@code Promise} from promises in sequence.
	 *
	 * @return {@code Promise} that completes when all promises are completed
	 */
	public static Promise<Void> runSequence(Iterator<? extends Promise<?>> promises) {
		SettablePromise<Void> result = new SettablePromise<>();
		runSequenceImpl(promises, result);
		return result;
	}

	private static void runSequenceImpl(Iterator<? extends Promise<?>> promises, SettablePromise<Void> cb) {
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

	@SafeVarargs
	public static <T, A, R> Promise<R> collectSequence(Collector<T, A, R> collector, AsyncSupplier<? extends T>... promises) {
		return collectSequence(collector, asList(promises));
	}

	/**
	 * @see Promises#collectSequence(Collector, Iterator)
	 */
	public static <T, A, R> Promise<R> collectSequence(Collector<T, A, R> collector, Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return collectSequence(collector, transform(promises.iterator(), AsyncSupplier::get));
	}

	/**
	 * @see Promises#collectSequence(Collector, Iterator)
	 */
	public static <T, A, R> Promise<R> collectSequence(Collector<T, A, R> collector, Stream<? extends Promise<? extends T>> promises) {
		return collectSequence(collector, promises.iterator());
	}

	/**
	 * Accumulate {@code Promise} results into one final using {@code Collector} sequentially
	 *
	 * @return new {@code Promise} that completes when all promises are completed
	 * @see Collector
	 */
	public static <T, A, R> Promise<R> collectSequence(Collector<T, A, R> collector, Iterator<? extends Promise<? extends T>> promises) {
		SettablePromise<R> result = new SettablePromise<>();
		collectSequenceImpl(promises, collector.accumulator(), collector.finisher(), collector.supplier().get(), result);
		return result;
	}

	private static <T, A, R> void collectSequenceImpl(Iterator<? extends Promise<? extends T>> promises,
			BiConsumer<A, T> accumulator, Function<A, R> finisher,
			A accumulatedValue, SettablePromise<R> cb) {
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
	 * Predicate in this case picks first {@code Promise} that was completed normally
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@SafeVarargs
	public static <T> Promise<T> firstSuccessful(AsyncSupplier<? extends T>... promises) {
		return first(isResult(), promises);
	}

	/**
	 * Predicate in this case picks first {@code Promise} that was completed normally
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> firstSuccessful(Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(isResult(), promises);
	}

	public static <T> Promise<T> firstSuccessful(Stream<? extends Promise<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * Predicate in this case picks first {@code Promise} that was completed normally
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> firstSuccessful(Iterator<? extends Promise<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@SafeVarargs
	public static <T> Promise<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			AsyncSupplier<? extends T>... promises) {
		return first(predicate, asList(promises));
	}

	public static <T> Promise<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, transform(promises.iterator(), AsyncSupplier::get));
	}

	public static <T> Promise<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			Stream<? extends Promise<? extends T>> promises) {
		return first(predicate, promises.iterator());
	}

	/**
	 * @param predicate for filtering results, consumes result of {@code Promise}
	 * @return first completed result of {@code Promise} that satisfies predicate
	 */
	public static <T> Promise<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			Iterator<? extends Promise<? extends T>> promises) {
		SettablePromise<T> cb = new SettablePromise<>();
		firstImpl(promises, predicate, cb);
		return cb;
	}

	/**
	 * Instead of returning new Promise, this method sets supplied promise when result collected.
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	private static <T> void firstImpl(Iterator<? extends Promise<? extends T>> promises,
			BiPredicate<? super T, ? super Throwable> predicate,
			SettablePromise<T> cb) {
		if (!promises.hasNext()) {
			cb.setException(new NoSuchElementException());
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

	public static <T> BiPredicate<T, Throwable> isResult() {
		return ($, e) -> e == null;
	}

	public static <T> BiPredicate<T, Throwable> isError() {
		return ($, e) -> e != null;
	}

	public static Promise<Void> repeat(Supplier<Promise<Void>> supplier) {
		SettablePromise<Void> cb = new SettablePromise<>();
		repeatImpl(supplier, cb);
		return cb;
	}

	private static void repeatImpl(Supplier<Promise<Void>> supplier, SettablePromise<Void> cb) {
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

	public static <T> Promise<Void> loop(T seed, Predicate<T> test, Function<T, Promise<T>> next) {
		if (!test.test(seed)) return Promise.complete();
		SettablePromise<Void> cb = new SettablePromise<>();
		loopImpl(seed, test, next, cb);
		return cb;
	}

	public static Promise<Void> loop(Predicate<Void> test, Function<Void, Promise<Void>> next) {
		return loop(null, test, next);
	}

	private static <T> void loopImpl(T seed, Predicate<T> test, Function<T, Promise<T>> next, SettablePromise<Void> cb) {
		while (true) {
			Promise<T> promise = next.apply(seed);
			if (promise.isResult()) {
				seed = promise.materialize().getResult();
				if (!test.test(seed)) break;
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

	// region helper classes
	private static final class PromiseAll<T> extends NextPromise<T, Void> {
		int countdown;

		@Override
		public void accept(@Nullable T result, Throwable e) {
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
		public void accept(@Nullable T result, Throwable e) {
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
		final Collector<T, A, R> collector;
		A accumulator;
		int countdown;

		private PromiseCollect(Collector<T, A, R> collector, A accumulator) {
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
		public void accept(@Nullable T result, Throwable e) {
			if (e == null) {
				processComplete(result);
			} else {
				tryCompleteExceptionally(e);
			}
		}
	}

	private static final class PromiseIndexedCollect<T, A, R> extends NextPromise<T, R> {
		final IndexedCollector<T, A, R> collector;
		A accumulator;
		int countdown;

		private PromiseIndexedCollect(IndexedCollector<T, A, R> collector, A accumulator) {
			this.collector = collector;
			this.accumulator = accumulator;
		}

		private PromiseIndexedCollect(Collector<T, A, R> collector, A accumulator) {
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
		public void accept(@Nullable T result, Throwable e) {
			if (e == null) {
				processComplete(result, 0);
			} else {
				tryCompleteExceptionally(e);
			}
		}

	}

	private static final class PromiseCollectEx<T, A, R> extends NextPromise<T, R> implements CollectListener.CollectCanceller {
		final IndexedCollector<T, A, R> collector;
		final CollectListener<T, A, R> listener;
		A accumulator;
		int countdown;

		private PromiseCollectEx(IndexedCollector<T, A, R> collector, CollectListener<T, A, R> listener, A accumulator) {
			this.collector = collector;
			this.listener = listener;
			this.accumulator = accumulator;
		}

		@Override
		public void accept(@Nullable T result, Throwable e) {
			if (e == null) {
				processComplete(result, 0);
			} else {
				processException(e, 0);
			}
		}

		void processComplete(@Nullable T promiseResult, int index) {
			if (isComplete()) {
				return;
			}
			collector.accumulate(accumulator, index, promiseResult);
			listener.onResult(promiseResult, index);
			if (--countdown == 0) {
				finish();
			}
		}

		@Override
		public void finish() {
			if (isComplete()) {
				return;
			}
			R finished = collector.finish(accumulator);
			//noinspection AssignmentToNull - resource release
			accumulator = null;
			listener.onCollectResult(finished);
			if (isComplete()) {
				return;
			}
			complete(finished);
		}

		void processException(Throwable e, int index) {
			if (isComplete()) {
				return;
			}
			listener.onException(e, index);
			finishExceptionally(e);
		}

		@Override
		public void finishExceptionally(Throwable e) {
			if (isComplete()) {
				return;
			}
			listener.onCollectException(e);
			if (isComplete()) {
				return;
			}
			completeExceptionally(e);
		}
	}

	static final class ReduceTimeouter<T, A, R> implements Runnable, CollectListener<T, A, R> {
		CollectCanceller canceller;
		ScheduledRunnable scheduledRunnable;

		@Override
		public void onStart(CollectCanceller canceller, A accumulator) {
			this.canceller = canceller;
		}

		@Override
		public void onCollectResult(R result) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
			}
		}

		@Override
		public void onCollectException(Throwable e) {
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
