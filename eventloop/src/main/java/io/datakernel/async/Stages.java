package io.datakernel.async;

import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.util.*;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.asIterator;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public final class Stages {
	private Stages() {
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all() {
		return Stage.of(null);
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
		if (size == 0) return Stage.of(null);
		if (size == 1) return stages.get(0).toVoid();
		if (size == 2) return stages.get(0).both(stages.get(1));
		return all(stages.iterator());
	}

	/**
	 * @return {@code Stage} that completes when all stages are completed
	 */
	public static Stage<Void> all(Iterator<? extends Stage<?>> stages) {
		if (!stages.hasNext()) return all();
		StageAll<Object> resultStage = new StageAll<>(1);
		stages.next().then(resultStage);
		while (stages.hasNext()) {
			resultStage.countdown++;
			stages.next().whenComplete((result, throwable) -> {
				if (throwable == null) {
					if (--resultStage.countdown == 0) {
						resultStage.complete(null);
					}
				} else {
					resultStage.tryCompleteExceptionally(throwable);
				}
			});
		}
		return resultStage;
	}

	private static final class StageAll<T> extends NextStage<T, Void> {
		int countdown;

		public StageAll(int countdown) {
			this.countdown = countdown;
		}

		@Override
		protected void onComplete(T result) {
			if (--countdown == 0) {
				complete(null);
			}
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			tryCompleteExceptionally(throwable);
		}
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
		Stage<? extends T> first = stages.next();
		StageAny<T> resultStage = new StageAny<>(1);
		first.then(resultStage);
		while (stages.hasNext()) {
			resultStage.errors++;
			stages.next().whenComplete((result, throwable) -> {
				if (throwable == null) {
					resultStage.tryComplete(result);
				} else {
					if (--resultStage.errors == 0) {
						resultStage.completeExceptionally(throwable);
					}
				}
			});
		}
		return resultStage;
	}

	private static final class StageAny<T> extends NextStage<T, T> {
		int errors;

		StageAny(int errors) {
			this.errors = errors;
		}

		@Override
		protected void onComplete(T result) {
			tryComplete(result);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			if (--errors == 0) {
				completeExceptionally(throwable);
			}
		}
	}

	public static <A, T, R> Stage<R> collect(List<? extends Stage<? extends T>> stages,
			Collector<T, A, R> collector) {
		return collect(stages, IndexedCollector.ofCollector(collector));
	}

	/**
	 * Accumulates results of {@code Stage}s using {@code IndexedCollector}.
	 *
	 * @param stages    collection of {@code Stage}s
	 * @param collector reducer which is used for combining {@code Stage} results into one value
	 * @param <T>       type of input value
	 * @param <A>       type of accumulator
	 * @param <R>       type of result
	 * @return {@code Stage} with accumulated result
	 * @see IndexedCollector
	 */
	public static <A, T, R> Stage<R> collect(List<? extends Stage<? extends T>> stages,
			IndexedCollector<T, A, R> collector) {
		int size = stages.size();
		if (size == 0) {
			return Stage.of(collector.resultOf());
		}
		if (size == 1) {
			return stages.get(0).thenApply(collector::resultOf);
		}
		if (size == 2) {
			return stages.get(0).combine(stages.get(1), collector::resultOf);
		}

		A accumulator = collector.accumulator(size);
		StageCollect<T, A, R> resultStage = new StageCollect<>(collector, accumulator, size);
		stages.get(0).then(resultStage);

		for (int i = 1; i < size; i++) {
			int index = i;
			stages.get(i).whenComplete((result, throwable) -> {
				if (throwable == null) {
					resultStage.processComplete(result, index);
				} else {
					resultStage.tryCompleteExceptionally(throwable);
				}
			});
		}
		return resultStage;
	}

	private static final class StageCollect<T, A, R> extends NextStage<T, R> {
		final IndexedCollector<T, A, R> collector;
		A accumulator;
		int stages;

		private StageCollect(IndexedCollector<T, A, R> collector, A accumulator, int stages) {
			this.collector = collector;
			this.accumulator = accumulator;
			this.stages = stages;
		}

		void processComplete(T result, int i) {
			if (isComplete()) {
				return;
			}
			collector.accumulate(accumulator, i, result);
			if (--stages == 0) {
				R reducerResult = collector.finish(accumulator);
				accumulator = null;
				complete(reducerResult);
			}
		}

		@Override
		protected void onComplete(T result) {
			processComplete(result, 0);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			tryCompleteExceptionally(throwable);
		}
	}

	/**
	 * Allows you to do something on completion of every {@code Stage}.
	 *
	 * @param listener calls {@link CollectListener#onCollectResult(Object)} with every {@code Stage} result
	 * @see Stages#collect(List, IndexedCollector)
	 */
	public static <A, T, R> Stage<R> collect(List<? extends Stage<? extends T>> stages,
			CollectListener<T, A, R> listener, IndexedCollector<T, A, R> collector) {
		int size = stages.size();
		if (size == 0) {
			R finished = collector.resultOf();
			listener.onCollectResult(finished);
			return Stage.of(finished);
		}

		A accumulator = collector.accumulator(size);
		StageCollectorEx<T, A, R> resultStage = new StageCollectorEx<>(collector, listener, accumulator, size);
		listener.onStart(resultStage, accumulator);
		stages.get(0).then(resultStage);

		for (int i = 1; i < size; i++) {
			int index = i;
			stages.get(i).whenComplete((result, throwable) -> {
				if (throwable == null) {
					resultStage.processComplete(result, index);
				} else {
					resultStage.processException(throwable, index);
				}
			});
		}
		return resultStage;
	}

	private static final class StageCollectorEx<T, A, R> extends NextStage<T, R> implements CollectListener.CollectCanceller {
		final IndexedCollector<T, A, R> collector;
		final CollectListener<T, A, R> listener;
		A accumulator;
		int countdown;

		private StageCollectorEx(IndexedCollector<T, A, R> collector, CollectListener<T, A, R> listener, A accumulator, int countdown) {
			this.collector = collector;
			this.listener = listener;
			this.accumulator = accumulator;
			this.countdown = countdown;
		}

		@Override
		protected void onComplete(T result) {
			processComplete(result, 0);
		}

		void processComplete(T stageResult, int index) {
			if (countdown <= 0) return;
			collector.accumulate(accumulator, index, stageResult);
			listener.onResult(stageResult, index);
			if (countdown == 1) {
				finish();
			} else {
				countdown--;
			}
		}

		@Override
		public void finish() {
			if (countdown <= 0) return;
			countdown = 0;
			R finished = collector.finish(accumulator);
			accumulator = null;
			listener.onCollectResult(finished);
			complete(finished);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			processException(throwable, 0);
		}

		void processException(Throwable throwable, int index) {
			if (countdown <= 0) return;
			listener.onException(throwable, index);
			finishExceptionally(throwable);
		}

		@Override
		public void finishExceptionally(Throwable throwable) {
			if (countdown <= 0) return;
			countdown = 0;
			accumulator = null;
			listener.onCollectException(throwable);
			completeExceptionally(throwable);
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
		public void onCollectException(Throwable throwable) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
			}
		}

		@Override
		public void run() {
			canceller.finish();
		}
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
	 * @see Stages#collect(List, IndexedCollector)
	 */
	public static <T> Stage<List<T>> toList(List<? extends Stage<? extends T>> stages) {
		return collect(stages, IndexedCollector.toList());
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
	 * @see Stages#collect(List, IndexedCollector)
	 */
	public static <T> Stage<T[]> toArray(Class<T> type, List<? extends Stage<? extends T>> stages) {
		return collect(stages, IndexedCollector.toArray(type));
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
		return Stage.of(null);
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncCallable<?> stage) {
		return stage.call().toVoid();
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncCallable<?> stage1, AsyncCallable<?> stage2) {
		return stage1.call().thenCompose($ -> runSequence(stage2));
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncCallable<?>... stages) {
		return runSequence(asList(stages));
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(AsyncCallable<?> stage1, AsyncCallable<?> stage2, AsyncCallable<?> stage3) {
		return stage1.call().thenCompose($ -> runSequence(stage2, stage3));
	}

	public static Stage<Void> runSequence(Stream<? extends AsyncCallable<?>> stages) {
		return runSequence(stages.iterator());
	}

	/**
	 * @see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(Iterable<? extends AsyncCallable<?>> stages) {
		return runSequence(stages.iterator());
	}

	/**
	 * Calls every {@code Stage} from stages in sequence.
	 *
	 * @return {@code Stage} that completes when all stages are completed
	 */
	public static Stage<Void> runSequence(Iterator<? extends AsyncCallable<?>> stages) {
		SettableStage<Void> result = SettableStage.create();
		runSequenceImpl(stages, result);
		return result;
	}

	private static void runSequenceImpl(Iterator<? extends AsyncCallable<?>> stages, SettableStage<Void> cb) {
		if (!stages.hasNext()) {
			cb.set(null);
			return;
		}
		stages.next().call().whenComplete((result, throwable) -> {
			if (throwable == null) {
				runSequenceImpl(stages, cb);
			} else {
				cb.setException(throwable);
			}
		});
	}

	/**
	 * @see Stages#collectSequence(Iterator, Collector)
	 */
	public static <T, A, R> Stage<R> collectSequence(Stream<AsyncCallable<? extends T>> stages,
			Collector<T, A, R> collector) {
		return collectSequence(stages.iterator(), collector);
	}

	/**
	 * @see Stages#collectSequence(Iterator, Collector)
	 */
	public static <T, A, R> Stage<R> collectSequence(Iterable<? extends AsyncCallable<? extends T>> stages,
			Collector<T, A, R> collector) {
		return collectSequence(stages.iterator(), collector);
	}

	/**
	 * Accumulate {@code Stage} results into one final using {@code Collector} sequentially
	 *
	 * @return new {@code Stage} that completes when all stages are completed
	 * @see Collector
	 */
	public static <T, A, R> Stage<R> collectSequence(Iterator<? extends AsyncCallable<? extends T>> stages,
			Collector<T, A, R> collector) {
		A accumulator = collector.supplier().get();
		return collectSequenceImpl(stages, collector, accumulator);
	}

	private static <T, A, R> Stage<R> collectSequenceImpl(Iterator<? extends AsyncCallable<? extends T>> stages,
			Collector<T, A, R> collector, A accumulator) {
		SettableStage<R> result = SettableStage.create();
		collectSequenceImpl(stages, collector, accumulator, result);
		return result;
	}

	private static <T, A, R> void collectSequenceImpl(Iterator<? extends AsyncCallable<? extends T>> stages,
			Collector<T, A, R> collector, A accumulator, SettableStage<R> cb) {
		if (!stages.hasNext()) {
			cb.set(collector.finisher().apply(accumulator));
			return;
		}
		stages.next().call().whenComplete((result, throwable) -> {
			if (throwable == null) {
				collector.accumulator().accept(accumulator, result);
				collectSequenceImpl(stages, collector, accumulator, cb);
			} else {
				cb.setException(throwable);
			}
		});
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 *
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	@SafeVarargs
	public static <T> Stage<T> firstSuccessful(AsyncCallable<? extends T>... stages) {
		return firstSuccessful(asList(stages));
	}

	public static <T> Stage<T> firstSuccessful(Stream<AsyncCallable<? extends T>> stages) {
		return firstSuccessful(stages.iterator());
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 *
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	public static <T> Stage<T> firstSuccessful(Iterable<? extends AsyncCallable<? extends T>> stages) {
		return firstSuccessful(stages.iterator());
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 *
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	public static <T> Stage<T> firstSuccessful(Iterator<? extends AsyncCallable<? extends T>> stages) {
		return first(stages, (result, throwable) -> throwable == null);
	}

	public static <T> Stage<T> first(Stream<? extends AsyncCallable<? extends T>> stages,
			BiPredicate<? super T, ? super Throwable> predicate) {
		return first(stages.iterator(), predicate);
	}

	/**
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	public static <T> Stage<T> first(Iterable<? extends AsyncCallable<? extends T>> stages,
			BiPredicate<? super T, ? super Throwable> predicate) {
		return first(stages.iterator(), predicate);
	}

	/**
	 * @param predicate for filtering results, consumes result of {@code Stage}
	 * @return first completed result of {@code Stage} that satisfies predicate
	 */
	public static <T> Stage<T> first(Iterator<? extends AsyncCallable<? extends T>> stages,
			BiPredicate<? super T, ? super Throwable> predicate) {
		SettableStage<T> cb = SettableStage.create();
		firstImpl(stages, predicate, cb);
		return cb;
	}

	/**
	 * Instead of returning new Stage, this method sets supplied stage when result collected.
	 *
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	private static <T> void firstImpl(Iterator<? extends AsyncCallable<? extends T>> stages,
			BiPredicate<? super T, ? super Throwable> predicate,
			SettableStage<T> cb) {
		if (!stages.hasNext()) {
			cb.setException(new NoSuchElementException());
			return;
		}
		AsyncCallable<? extends T> callable = stages.next();
		callable.call().whenComplete((result, throwable) -> {
			if (predicate.test(result, throwable)) {
				cb.set(result, throwable);
				return;
			}
			firstImpl(stages, predicate, cb);
		});
	}

	/**
	 * Transforms Iterable over AsyncCallable to Iterable over Stages
	 */
	public static <T> Iterable<Stage<T>> iterable(Iterable<? extends AsyncCallable<? extends T>> callables) {
		return () -> iterator(callables.iterator());
	}

	/**
	 * Transforms Iterator over AsyncCallable to Iterator over Stages
	 *
	 * @param callables Iterator over AsyncCallable
	 * @return new Iterator over Stage
	 */
	public static <T> Iterator<Stage<T>> iterator(Iterator<? extends AsyncCallable<? extends T>> callables) {
		return new Iterator<Stage<T>>() {
			@Override
			public boolean hasNext() {
				return callables.hasNext();
			}

			@SuppressWarnings("unchecked")
			@Override
			public Stage<T> next() {
				return (Stage<T>) callables.next().call();
			}
		};
	}
}
