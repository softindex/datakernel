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

public final class Stages {
	private Stages() {}

	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException(Stages.class, "Stage timeout");

	public static <T> Stage<T> timeout(Stage<T> stage, long delay) {
		if (stage.isComplete()) return stage;
		return stage.then(new NextStage<T, T>() {
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

	public static <T> Stage<T> timeout(Stage<T> stage, Duration delay) {
		return timeout(stage, delay.toMillis());
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all() {
		return Stage.complete();
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stage<?> stage1) {
		return stage1.toVoid();
	}

	/**
	 * Optimized for 2 stages.
	 *
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stage<?> stage1, Stage<?> stage2) {
		return stage1.both(stage2);
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stage<?>... stages) {
		return all(asIterator(stages));
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stream<? extends Stage<?>> stages) {
		return all(stages.iterator());
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Iterable<? extends Stage<?>> stages) {
		return all(stages.iterator());
	}

	/**
	 * @return {@code Stage} that completes when all stages are completed
	 */
	public static Stage<Void> all(List<? extends Stage<?>> stages) {
		int size = stages.size();
		if (size == 0) return Stage.complete();
		if (size == 1) return stages.get(0).toVoid();
		if (size == 2) return stages.get(0).both(stages.get(1));
		return all(stages.iterator());
	}

	/**
	 * @return {@code Stage} that completes when all stages are completed
	 */
	public static Stage<Void> all(Iterator<? extends Stage<?>> stages) {
		if (!stages.hasNext()) return all();
		StageAll<Object> resultStage = new StageAll<>();
		while (stages.hasNext()) {
			Stage<?> stage = stages.next();
			if (stage.isResult()) continue;
			if (stage.isException()) return Stage.ofException(stage.getException());
			resultStage.countdown++;
			stage.then(resultStage);
		}
		return resultStage.countdown != 0 ? resultStage : Stage.complete();
	}

	public static <T> Stage<T> any() {
		return Stage.ofException(new NoSuchElementException());
	}

	/**
	 * @see Stages#any(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(Stage<? extends T> stage1) {
		return (Stage<T>) stage1;
	}

	/**
	 * Optimized for 2 stages.
	 *
	 * @see Stages#any(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return ((Stage<T>) stage1).either(stage2);
	}

	/**
	 * @see Stages#any(List)
	 */
	@SafeVarargs
	public static <T> Stage<T> any(Stage<? extends T>... stages) {
		return any(asIterator(stages));
	}

	public static <T> Stage<T> any(Stream<? extends Stage<? extends T>> stages) {
		return any(stages.iterator());
	}

	public static <T> Stage<T> any(Iterable<? extends Stage<? extends T>> stages) {
		return any(stages.iterator());
	}

	/**
	 * @return first completed stage
	 * this method returns one of the first completed stages, because it's async we can't really get FIRST completed stage.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(List<? extends Stage<? extends T>> stages) {
		int size = stages.size();
		if (size == 1) return (Stage<T>) stages.get(0);
		if (size == 2) return ((Stage<T>) stages.get(0)).either(stages.get(1));
		return any(stages.iterator());
	}

	public static <T> Stage<T> any(Iterator<? extends Stage<? extends T>> stages) {
		if (!stages.hasNext()) return any();
		StageAny<T> resultStage = new StageAny<>();
		while (stages.hasNext()) {
			Stage<? extends T> stage = stages.next();
			if (stage.isResult()) return Stage.of(stage.getResult());
			if (stage.isException()) continue;
			resultStage.errors++;
			stage.whenComplete((result, e) -> {
				if (e == null) {
					resultStage.tryComplete(result);
				} else {
					if (--resultStage.errors == 0) {
						resultStage.completeExceptionally(e);
					}
				}
			});
		}
		return resultStage.errors != 0 ? resultStage : any();
	}

	/**
	 * Accumulates results of {@code Stage}s using {@code IndexedCollector}.
	 *
	 * @param <T>       type of input value
	 * @param <A>       type of accumulator
	 * @param <R>       type of result
	 * @param collector reducer which is used for combining {@code Stage} results into one value
	 * @param stages    collection of {@code Stage}s
	 * @return {@code Stage} with accumulated result
	 * @see IndexedCollector
	 */
	public static <T, A, R> Stage<R> collect(IndexedCollector<T, A, R> collector, List<? extends Stage<? extends T>> stages) {
		int size = stages.size();
		if (size == 0) return Stage.of(collector.resultOf());
		if (size == 1) return stages.get(0).thenApply(collector::resultOf);
		if (size == 2) return stages.get(0).combine(stages.get(1), collector::resultOf);

		A accumulator = collector.accumulator(size);
		StageIndexedCollect<T, A, R> resultStage = new StageIndexedCollect<>(collector, accumulator);

		for (int i = 0; i < size; i++) {
			Stage<? extends T> stage = stages.get(i);
			if (stage.isResult()) {
				collector.accumulate(resultStage.accumulator, i, stage.getResult());
				continue;
			}
			if (stage.isException()) return Stage.ofException(stage.getException());
			int index = i;
			resultStage.countdown++;
			stage.whenComplete((result, e) -> {
				if (e == null) {
					resultStage.processComplete(result, index);
				} else {
					resultStage.tryCompleteExceptionally(e);
				}
			});
		}
		return resultStage.countdown != 0 ? resultStage : Stage.of(collector.finish(resultStage.accumulator));
	}

	/**
	 * Allows you to do something on completion of every {@code Stage}.
	 *
	 * @param listener calls {@link CollectListener#onCollectResult(Object)} with every {@code Stage} result
	 * @see Stages#collect(IndexedCollector, List)
	 */
	public static <T, A, R> Stage<R> collect(IndexedCollector<T, A, R> collector, CollectListener<T, A, R> listener, List<? extends Stage<? extends T>> stages) {
		int size = stages.size();
		if (size == 0) {
			R finished = collector.resultOf();
			listener.onCollectResult(finished);
			return Stage.of(finished);
		}

		A accumulator = collector.accumulator(size);
		StageCollectEx<T, A, R> resultStage = new StageCollectEx<>(collector, listener, accumulator);
		listener.onStart(resultStage, accumulator);

		for (int i = 0; i < size; i++) {
			Stage<? extends T> stage = stages.get(i);
			if (stage.isResult()) {
				collector.accumulate(resultStage.accumulator, i, stage.getResult());
				continue;
			}
			if (stage.isException()) return Stage.ofException(stage.getException());
			int index = i;
			resultStage.countdown++;
			stage.whenComplete((result, e) -> {
				if (e == null) {
					resultStage.processComplete(result, index);
				} else {
					resultStage.processException(e, index);
				}
			});
		}
		if (resultStage.countdown == 0) {
			R result = collector.finish(resultStage.accumulator);
			listener.onCollectResult(result);
			return Stage.of(result);
		} else {
			return resultStage;
		}
	}

	public static <T, A, R> Stage<R> collect(Collector<T, A, R> collector, Iterator<? extends Stage<? extends T>> stages) {
		A accumulatorValue = collector.supplier().get();
		BiConsumer<A, T> accumulatorConsumer = collector.accumulator();
		if (!stages.hasNext()) {
			return Stage.of(collector.finisher().apply(accumulatorValue));
		}
		StageCollect<T, A, R> resultStage = new StageCollect<>(collector, accumulatorValue);

		while (stages.hasNext()) {
			Stage<? extends T> stage = stages.next();
			if (stage.isResult()) {
				accumulatorConsumer.accept(resultStage.accumulator, stage.getResult());
				continue;
			}
			if (stage.isException()) return Stage.ofException(stage.getException());
			resultStage.countdown++;
			stage.then(resultStage);
		}
		return resultStage.countdown != 0 ? resultStage : Stage.of(collector.finisher().apply(resultStage.accumulator));
	}

	public static <T, A, R> Stage<R> collect(Collector<T, A, R> collector, Iterable<? extends Stage<? extends T>> stages) {
		return collect(collector, stages.iterator());
	}

	public static <T, A, R> Stage<R> collect(Collector<T, A, R> collector, Stream<? extends Stage<? extends T>> stages) {
		return collect(collector, stages.iterator());
	}

	public static <T> Stage<List<T>> toList() {
		return Stage.of(emptyList());
	}

	/**
	 * @see Stages#toList(Stage[])
	 */
	public static <T> Stage<List<T>> toList(Stage<? extends T> stage1) {
		return stage1.thenApply(Arrays::asList);
	}

	/**
	 * @see Stages#toList(Stage[])
	 */
	public static <T> Stage<List<T>> toList(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.combine(stage2, (value1, value2) -> asList(value1, value2));
	}

	/**
	 * @see Stages#toList(Stage[])
	 */
	@SafeVarargs
	public static <T> Stage<List<T>> toList(Stage<? extends T>... stages) {
		return toList(asList(stages));
	}

	/**
	 * @see Stages#toList(Stage[])
	 */
	public static <T> Stage<List<T>> toList(Stream<? extends Stage<? extends T>> stages) {
		List<Stage<? extends T>> list = stages.collect(Collectors.toList());
		return toList(list);
	}

	/**
	 * Prepared version of reduce that reduces stages into Stage&lt;List&gt;
	 *
	 * @see Stages#collect(IndexedCollector, List)
	 */
	public static <T> Stage<List<T>> toList(List<? extends Stage<? extends T>> stages) {
		return collect(IndexedCollector.toList(), stages);
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T[]> toArray(Class<T> type) {
		return Stage.of((T[]) Array.newInstance(type, 0));
	}

	/**
	 * @see Stages#toArray(Class, List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T[]> toArray(Class<T> type, Stage<? extends T> stage1) {
		return stage1.thenApply(value -> {
			T[] array = (T[]) Array.newInstance(type, 1);
			array[0] = value;
			return array;
		});
	}

	/**
	 * @see Stages#toArray(Class, List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T[]> toArray(Class<T> type, Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.combine(stage2, (value1, value2) -> {
			T[] array = (T[]) Array.newInstance(type, 2);
			array[0] = value1;
			array[1] = value2;
			return array;
		});
	}

	/**
	 * @see Stages#toArray(Class, List)
	 */
	@SafeVarargs
	public static <T> Stage<T[]> toArray(Class<T> type, Stage<? extends T>... stages) {
		return toArray(type, asList(stages));
	}

	/**
	 * @see Stages#toArray(Class, List)
	 */
	public static <T> Stage<T[]> toArray(Class<T> type, Stream<? extends Stage<? extends T>> stages) {
		List<Stage<? extends T>> list = stages.collect(Collectors.toList());
		return toArray(type, list);
	}

	/**
	 * Prepared version of reduce that reduces stages into Stage&lt;Array&gt;
	 *
	 * @see Stages#collect(IndexedCollector, List)
	 */
	public static <T> Stage<T[]> toArray(Class<T> type, List<? extends Stage<? extends T>> stages) {
		return collect(IndexedCollector.toArray(type), stages);
	}

	public static <T1, R> Stage<R> toTuple(TupleConstructor1<T1, R> constructor, Stage<? extends T1> stage1) {
		return stage1.thenApply(constructor::create);
	}

	public static <T1, T2, R> Stage<R> toTuple(TupleConstructor2<T1, T2, R> constructor,
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2) {
		return stage1.combine(stage2, constructor::create);
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, R> Stage<R> toTuple(TupleConstructor3<T1, T2, T3, R> constructor,
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3) {
		return toList(stage1, stage2, stage3)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, R> Stage<R> toTuple(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3,
			Stage<? extends T4> stage4) {
		return toList(stage1, stage2, stage3, stage4)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, R> Stage<R> toTuple(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3,
			Stage<? extends T4> stage4,
			Stage<? extends T5> stage5) {
		return toList(stage1, stage2, stage3, stage4, stage5)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, R> Stage<R> toTuple(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3,
			Stage<? extends T4> stage4,
			Stage<? extends T5> stage5,
			Stage<? extends T6> stage6) {
		return toList(stage1, stage2, stage3, stage4, stage5, stage6)
				.thenApply(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	public static <T1> Stage<Tuple1<T1>> toTuple(Stage<? extends T1> stage1) {
		return stage1.thenApply((Function<T1, Tuple1<T1>>) Tuple1::new);
	}

	public static <T1, T2> Stage<Tuple2<T1, T2>> toTuple(Stage<? extends T1> stage1, Stage<? extends T2> stage2) {
		return stage1.combine(stage2, (BiFunction<T1, T2, Tuple2<T1, T2>>) Tuple2::new);
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Stage<Tuple3<T1, T2, T3>> toTuple(
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3) {
		return toList(stage1, stage2, stage3)
				.thenApply(list -> new Tuple3<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Stage<Tuple4<T1, T2, T3, T4>> toTuple(
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3,
			Stage<? extends T4> stage4) {
		return toList(stage1, stage2, stage3, stage4)
				.thenApply(list -> new Tuple4<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Stage<Tuple5<T1, T2, T3, T4, T5>> toTuple(
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3,
			Stage<? extends T4> stage4,
			Stage<? extends T5> stage5) {
		return toList(stage1, stage2, stage3, stage4, stage5)
				.thenApply(list -> new Tuple5<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Stage<Tuple6<T1, T2, T3, T4, T5, T6>> toTuple(
			Stage<? extends T1> stage1,
			Stage<? extends T2> stage2,
			Stage<? extends T3> stage3,
			Stage<? extends T4> stage4,
			Stage<? extends T5> stage5,
			Stage<? extends T6> stage6) {
		return toList(stage1, stage2, stage3, stage4, stage5, stage6)
				.thenApply(list -> new Tuple6<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence() {
		return Stage.complete();
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncSupplier<?> stage) {
		return stage.get().toVoid();
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncSupplier<?> stage1, AsyncSupplier<?> stage2) {
		return stage1.get().thenCompose($ -> runSequence(stage2));
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncSupplier<?>... stages) {
		return runSequence(asList(stages));
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncSupplier<?> stage1, AsyncSupplier<?> stage2, AsyncSupplier<?> stage3) {
		return stage1.get().thenCompose($ -> runSequence(stage2, stage3));
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(Iterable<? extends AsyncSupplier<?>> stages) {
		return runSequence(transform(stages.iterator(), AsyncSupplier::get));
	}

	public static Stage<Void> runSequence(Stream<? extends Stage<?>> stages) {
		return runSequence(stages.iterator());
	}

	/**
	 * Calls every {@code Stage} from stages in sequence.
	 *
	 * @return {@code Stage} that completes when all stages are completed
	 */
	public static Stage<Void> runSequence(Iterator<? extends Stage<?>> stages) {
		SettableStage<Void> result = new SettableStage<>();
		runSequenceImpl(stages, result);
		return result;
	}

	private static void runSequenceImpl(Iterator<? extends Stage<?>> stages, SettableStage<Void> cb) {
		if (!stages.hasNext()) {
			cb.set(null);
			return;
		}
		stages.next().whenComplete((result, e) -> {
			if (e == null) {
				runSequenceImpl(stages, cb);
			} else {
				cb.setException(e);
			}
		});
	}

	@SafeVarargs
	public static <T, A, R> Stage<R> collectSequence(Collector<T, A, R> collector, AsyncSupplier<? extends T>... stages) {
		return collectSequence(collector, asList(stages));
	}

	/**
	 * @see Stages#collectSequence(Collector, Iterator)
	 */
	public static <T, A, R> Stage<R> collectSequence(Collector<T, A, R> collector, Iterable<? extends AsyncSupplier<? extends T>> stages) {
		return collectSequence(collector, transform(stages.iterator(), AsyncSupplier::get));
	}

	/**
	 * @see Stages#collectSequence(Collector, Iterator)
	 */
	public static <T, A, R> Stage<R> collectSequence(Collector<T, A, R> collector, Stream<? extends Stage<? extends T>> stages) {
		return collectSequence(collector, stages.iterator());
	}

	/**
	 * Accumulate {@code Stage} results into one final using {@code Collector} sequentially
	 *
	 * @return new {@code Stage} that completes when all stages are completed
	 * @see Collector
	 */
	public static <T, A, R> Stage<R> collectSequence(Collector<T, A, R> collector, Iterator<? extends Stage<? extends T>> stages) {
		SettableStage<R> result = new SettableStage<>();
		collectSequenceImpl(stages, collector.accumulator(), collector.finisher(), collector.supplier().get(), result);
		return result;
	}

	private static <T, A, R> void collectSequenceImpl(Iterator<? extends Stage<? extends T>> stages,
			BiConsumer<A, T> accumulator, Function<A, R> finisher,
			A accumulatedValue, SettableStage<R> cb) {
		if (!stages.hasNext()) {
			cb.set(finisher.apply(accumulatedValue));
			return;
		}
		stages.next().whenComplete((result, e) -> {
			if (e == null) {
				accumulator.accept(accumulatedValue, result);
				collectSequenceImpl(stages, accumulator, finisher, accumulatedValue, cb);
			} else {
				cb.setException(e);
			}
		});
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 *
	 * @see Stages#first(BiPredicate, Iterator)
	 */
	@SafeVarargs
	public static <T> Stage<T> firstSuccessful(AsyncSupplier<? extends T>... stages) {
		return first(isResult(), stages);
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 *
	 * @see Stages#first(BiPredicate, Iterator)
	 */
	public static <T> Stage<T> firstSuccessful(Iterable<? extends AsyncSupplier<? extends T>> stages) {
		return first(isResult(), stages);
	}

	public static <T> Stage<T> firstSuccessful(Stream<? extends Stage<? extends T>> stages) {
		return first(isResult(), stages);
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 *
	 * @see Stages#first(BiPredicate, Iterator)
	 */
	public static <T> Stage<T> firstSuccessful(Iterator<? extends Stage<? extends T>> stages) {
		return first(isResult(), stages);
	}

	/**
	 * @see Stages#first(BiPredicate, Iterator)
	 */
	@SafeVarargs
	public static <T> Stage<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			AsyncSupplier<? extends T>... stages) {
		return first(predicate, asList(stages));
	}

	public static <T> Stage<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			Iterable<? extends AsyncSupplier<? extends T>> stages) {
		return first(predicate, transform(stages.iterator(), AsyncSupplier::get));
	}

	public static <T> Stage<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			Stream<? extends Stage<? extends T>> stages) {
		return first(predicate, stages.iterator());
	}

	/**
	 * @param predicate for filtering results, consumes result of {@code Stage}
	 * @return first completed result of {@code Stage} that satisfies predicate
	 */
	public static <T> Stage<T> first(BiPredicate<? super T, ? super Throwable> predicate,
			Iterator<? extends Stage<? extends T>> stages) {
		SettableStage<T> cb = new SettableStage<>();
		firstImpl(stages, predicate, cb);
		return cb;
	}

	/**
	 * Instead of returning new Stage, this method sets supplied stage when result collected.
	 *
	 * @see Stages#first(BiPredicate, Iterator)
	 */
	private static <T> void firstImpl(Iterator<? extends Stage<? extends T>> stages,
			BiPredicate<? super T, ? super Throwable> predicate,
			SettableStage<T> cb) {
		if (!stages.hasNext()) {
			cb.setException(new NoSuchElementException());
			return;
		}
		stages.next().whenComplete((result, e) -> {
			if (predicate.test(result, e)) {
				cb.set(result, e);
				return;
			}
			firstImpl(stages, predicate, cb);
		});
	}

	public static <T> BiPredicate<T, Throwable> isResult() {
		return ($, e) -> e == null;
	}

	public static <T> BiPredicate<T, Throwable> isError() {
		return ($, e) -> e != null;
	}

	public static Stage<Void> repeat(Supplier<Stage<Void>> supplier) {
		SettableStage<Void> cb = new SettableStage<>();
		repeatImpl(supplier, cb);
		return cb;
	}

	private static void repeatImpl(Supplier<Stage<Void>> supplier, SettableStage<Void> cb) {
		while (true) {
			Stage<Void> stage = supplier.get();
			if (!stage.hasResult()) {
				stage.whenComplete(($, e) -> {
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

	public static <T> Stage<Void> loop(T seed, Predicate<T> test, Function<T, Stage<T>> next) {
		if (!test.test(seed)) return Stage.complete();
		SettableStage<Void> cb = new SettableStage<>();
		loopImpl(seed, test, next, cb);
		return cb;
	}

	private static <T> void loopImpl(T seed, Predicate<T> test, Function<T, Stage<T>> next, SettableStage<Void> cb) {
		while (true) {
			Stage<T> stage = next.apply(seed);
			if (stage.hasResult()) {
				seed = stage.getResult();
				if (!test.test(seed)) break;
			} else {
				stage.whenComplete((newSeed, e) -> {
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
	private static final class StageAll<T> extends NextStage<T, Void> {
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

	private static final class StageAny<T> extends NextStage<T, T> {
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

	private static final class StageCollect<T, A, R> extends NextStage<T, R> {
		final Collector<T, A, R> collector;
		A accumulator;
		int countdown;

		private StageCollect(Collector<T, A, R> collector, A accumulator) {
			this.collector = collector;
			this.accumulator = accumulator;
		}

		void processComplete(T result) {
			if (isComplete()) {
				return;
			}
			collector.accumulator().accept(accumulator, result);
			if (--countdown == 0) {
				R reducerResult = collector.finisher().apply(accumulator);
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

	private static final class StageIndexedCollect<T, A, R> extends NextStage<T, R> {
		final IndexedCollector<T, A, R> collector;
		A accumulator;
		int countdown;

		private StageIndexedCollect(IndexedCollector<T, A, R> collector, A accumulator) {
			this.collector = collector;
			this.accumulator = accumulator;
		}

		private StageIndexedCollect(Collector<T, A, R> collector, A accumulator) {
			this.collector = IndexedCollector.ofCollector(collector);
			this.accumulator = accumulator;
		}

		void processComplete(T result, int i) {
			if (isComplete()) {
				return;
			}
			collector.accumulate(accumulator, i, result);
			if (--countdown == 0) {
				R reducerResult = collector.finish(accumulator);
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

	private static final class StageCollectEx<T, A, R> extends NextStage<T, R> implements CollectListener.CollectCanceller {
		final IndexedCollector<T, A, R> collector;
		final CollectListener<T, A, R> listener;
		A accumulator;
		int countdown;

		private StageCollectEx(IndexedCollector<T, A, R> collector, CollectListener<T, A, R> listener, A accumulator) {
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

		void processComplete(T stageResult, int index) {
			if (isComplete()) {
				return;
			}
			collector.accumulate(accumulator, index, stageResult);
			listener.onResult(stageResult, index);
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
