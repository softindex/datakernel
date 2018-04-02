package io.datakernel.async;

import io.datakernel.eventloop.ScheduledRunnable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;

public final class Stages {
	private Stages() {
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

	/**
	 * @return {@code Stage} that completes when all stages are completed
	 */
	public static Stage<Void> all(List<? extends Stage<?>> stages) {
		int size = stages.size();
		if (size == 0) return Stage.of(null);
		if (size == 1) return stages.get(0).toVoid();
		if (size == 2) return stages.get(0).both(stages.get(1));
		StageAll<Object> resultStage = new StageAll<>(stages.size());
		stages.get(0).then(resultStage);
		for (int i = 1; i < size; i++) {
			stages.get(i).whenComplete((result, throwable) -> {
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

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stream<? extends Stage<Void>> stages) {
		return all(stages.collect(Collectors.toList()));
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stage<?>... stages) {
		return all(asList(stages));
	}

	/**
	 * Optimized for 2 stages.
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stage<?> stage1, Stage<?> stage2) {
		return stage1.both(stage2);
	}

	/**
	 * @see Stages#all(List)
	 */
	public static Stage<Void> all(Stage<?> stage) {
		return stage.toVoid();
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

	/**
	 * @return first completed stage
	 * @implNote this method returns one of the first completed stages, because it's async we can't really get FIRST completed stage.
	 * So you get one of the first completed stages.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(List<? extends Stage<? extends T>> stages) {
		int size = stages.size();
		checkArgument(size != 0);
		if (size == 1) return (Stage<T>) stages.get(0);
		if (size == 2) return ((Stage<T>) stages.get(0)).either(stages.get(1));
		StageAny<T> resultStage = new StageAny<>(size);
		stages.get(0).then(resultStage);
		for (int i = 1; i < size; i++) {
			stages.get(i).whenComplete((result, throwable) -> {
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

	/**
	 * @see Stages#any(List)
	 */
	@SafeVarargs
	public static <T> Stage<T> any(Stage<? extends T>... stages) {
		return any(asList(stages));
	}

	/**
	 * Optimized for 2 stages.
	 * @see Stages#any(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return ((Stage<T>) stage1).either(stage2);
	}

	/**
	 * @see Stages#any(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(Stage<? extends T> stage) {
		return (Stage<T>) stage;
	}

	private static final class StageReduce<T, A, R> extends NextStage<T, R> {
		final IndexedCollector<T, A, R> reducer;
		A accumulator;
		int stages;

		private StageReduce(IndexedCollector<T, A, R> reducer, A accumulator, int stages) {
			this.reducer = reducer;
			this.accumulator = accumulator;
			this.stages = stages;
		}

		void processComplete(T result, int i) {
			if (isComplete()) return;
			reducer.accumulate(accumulator, i, result);
			if (--stages == 0) {
				R reducerResult = reducer.finish(accumulator);
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
	 * Accumulates results of {@code Stage}s using {@code IndexedCollector}.
	 * @param stages collection of {@code Stage}s
	 * @param collector reducer which is used for combining {@code Stage} results into one value
	 * @param <T> type of input value
	 * @param <A> type of accumulator
	 * @param <R> type of result
	 * @return {@code Stage} with accumulated result
     * @see IndexedCollector
	 */
	public static <A, T, R> Stage<R> reduce(List<? extends Stage<? extends T>> stages,
	                                        IndexedCollector<T, A, R> collector) {
		int size = stages.size();
		if (size == 0) return Stage.of(collector.resultOf());
		if (size == 1) return stages.get(0).thenApply(collector::resultOf);
		if (size == 2) return stages.get(0).combine(stages.get(1), collector::resultOf);

		A accumulator = collector.accumulator(size);
		StageReduce<T, A, R> resultStage = new StageReduce<>(collector, accumulator, size);
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

	static final class ReduceTimeouter<T, A, R> implements Runnable, ReduceListener<T, A, R> {
		ReduceCanceller canceller;
		ScheduledRunnable scheduledRunnable;

		@Override
		public void onStart(ReduceCanceller canceller, A accumulator) {
			this.canceller = canceller;
		}

		@Override
		public void onReducerResult(R result) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
			}
		}

		@Override
		public void onReducerException(Throwable throwable) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
			}
		}

		@Override
		public void run() {
			canceller.finish();
		}
	}

	private static final class StageReduceEx<T, A, R> extends NextStage<T, R> implements ReduceListener.ReduceCanceller {
		final IndexedCollector<T, A, R> reducer;
		final ReduceListener<T, A, R> listener;
		A accumulator;
		int stages;

		private StageReduceEx(IndexedCollector<T, A, R> reducer, ReduceListener<T, A, R> listener, A accumulator, int stages) {
			this.reducer = reducer;
			this.listener = listener;
			this.accumulator = accumulator;
			this.stages = stages;
		}

		@Override
		protected void onComplete(T result) {
			processComplete(result, 0);
		}

		void processComplete(T stageResult, int index) {
			if (isComplete()) return;
			reducer.accumulate(accumulator, index, stageResult);
			listener.onResult(stageResult, index);
			if (--stages == 0) {
				finish();
			}
		}

		@Override
		public void finish() {
			if (isComplete()) return;
			R finished = reducer.finish(accumulator);
			accumulator = null;
			listener.onReducerResult(finished);
			if (isComplete()) return;
			complete(finished);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			processException(throwable, 0);
		}

		void processException(Throwable throwable, int index) {
			if (isComplete()) return;
			listener.onException(throwable, index);
			finishExceptionally(throwable);
		}

		@Override
		public void finishExceptionally(Throwable throwable) {
			if (isComplete()) return;
			listener.onReducerException(throwable);
			if (isComplete()) return;
			completeExceptionally(throwable);
		}
	}

	/**
	 * Allows you to do something on completion of every {@code Stage}.
	 * @param listener calls {@link ReduceListener#onReducerResult(Object)} with every {@code Stage} result
     * @see Stages#reduce(List, IndexedCollector)
	 */
	public static <A, T, R> Stage<R> reduce(List<? extends Stage<? extends T>> stages, ReduceListener<T, A, R> listener,
	                                        IndexedCollector<T, A, R> collector) {
		int size = stages.size();
		if (size == 0) {
			R finished = collector.resultOf();
			listener.onReducerResult(finished);
			return Stage.of(finished);
		}

		A accumulator = collector.accumulator(size);
		StageReduceEx<T, A, R> resultStage = new StageReduceEx<>(collector, listener, accumulator, size);
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

	/**
     * Prepared version of reduce that reduces stages into Stage&lt;List&gt;
	 * @see Stages#reduce(List, IndexedCollector)
	 */
	public static <T> Stage<List<T>> reduceToList(List<? extends Stage<? extends T>> stages) {
		return reduce(stages, IndexedCollector.toList());
	}

	/**
	 * @see Stages#reduceToList(Stage[])
	 */
	public static <T> Stage<List<T>> reduceToList(Stream<? extends Stage<? extends T>> stages) {
		List<? extends Stage<? extends T>> list = stages.collect(Collectors.toList());
		return reduceToList(list);
	}

	/**
	 * @see Stages#reduceToList(Stage[])
	 */
	@SafeVarargs
	public static <T> Stage<List<T>> reduceToList(Stage<? extends T>... stages) {
		return reduceToList(asList(stages));
	}

	/**
	 * @see Stages#reduceToList(Stage[])
	 */
	public static <T> Stage<List<T>> reduceToList(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.combine(stage2, (value1, value2) -> asList(value1, value2));
	}

	/**
	 * @see Stages#reduceToList(Stage[])
	 */
	public static <T> Stage<List<T>> reduceToList(Stage<? extends T> stage1) {
		return stage1.thenApply(Collections::singletonList);
	}

	/**
	 * Prepared version of reduce that reduces stages into Stage&lt;Array&gt;
	 * @see Stages#reduce(List, IndexedCollector)
	 */
	public static <T> Stage<T[]> reduceToArray(List<? extends Stage<? extends T>> stages) {
		return reduce(stages, IndexedCollector.toArray());
	}

	/**
	 * @see Stages#reduceToArray(List)
	 */
	public static <T> Stage<T[]> reduceToArray(Stream<? extends Stage<? extends T>> stages) {
		List<? extends Stage<? extends T>> list = stages.collect(Collectors.toList());
		return reduceToArray(list);
	}

	/**
	 * @see Stages#reduceToArray(List)
	 */
	@SafeVarargs
	public static <T> Stage<T[]> reduceToArray(Stage<? extends T>... stages) {
		return reduceToArray(asList(stages));
	}

	/**
	 * @see Stages#reduceToArray(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T[]> reduceToArray(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.combine(stage2, (value1, value2) -> (T[]) new Object[]{value1, value2});
	}

	/**
	 * @see Stages#reduceToArray(List)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T[]> reduceToArray(Stage<? extends T> stage1) {
		return stage1.thenApply(value -> (T[]) new Object[]{value});
	}

	/**
	 * Calls every {@code Stage} from stages in sequence.
	 * @return {@code Stage} that completes when all stages are completed
	 */
	public static Stage<Void> runSequence(Iterator<? extends AsyncCallable<?>> stages) {
		SettableStage<Void> result = new SettableStage<>();
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
	 *	@see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(Iterable<? extends AsyncCallable<?>> stages) {
		return runSequence(stages.iterator());
	}

	/**
	 *	@see Stages#runSequence(Iterator)
	 */
	public static Stage<Void> runSequence(Stream<? extends AsyncCallable<?>> stages) {
		return runSequence(stages.iterator());
	}

	/**
	 *	@see Stages#runSequence(Iterator)
	 */
	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable... stages) {
		return runSequence((AsyncCallable) asList(stages));
	}

	/**
	 *	@see Stages#runSequence(Iterator)
	 */
	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable stage) {
		return stage.call().toVoid();
	}

	/**
	 *	@see Stages#runSequence(Iterator)
	 */
	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable stage1, AsyncCallable stage2) {
		return stage1.call().thenCompose($ -> runSequence(stage2));
	}

	/**
	 *	@see Stages#runSequence(Iterator)
	 */
	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable stage1, AsyncCallable stage2, AsyncCallable stage3) {
		return stage1.call().thenCompose($ -> runSequence(stage2, stage3));
	}

	private static <T, A, R> Stage<R> reduceSequenceImpl(Iterator<? extends AsyncCallable<? extends T>> stages, A accumulator,
	                                                     Collector<T, A, R> collector) {
		SettableStage<R> result = SettableStage.create();
		reduceSequenceImpl(stages, accumulator, collector, result);
		return result;
	}

	private static <T, A, R> void reduceSequenceImpl(Iterator<? extends AsyncCallable<? extends T>> stages, A accumulator,
	                                                 Collector<T, A, R> collector, SettableStage<R> cb) {
		if (!stages.hasNext()) {
			cb.set(collector.finisher().apply(accumulator));
			return;
		}
		stages.next().call().whenComplete((result, throwable) -> {
			if (throwable == null) {
				collector.accumulator().accept(accumulator, result);
				reduceSequenceImpl(stages, accumulator, collector, cb);
			} else {
				cb.setException(throwable);
			}
		});
	}

	/**
	 * Accumulate {@code Stage} results into one final using {@code Collector} sequentially
	 * @return new {@code Stage} that completes when all stages are completed
	 * @see Collector
	 */
	public static <T, A, R> Stage<R> reduceSequence(Iterator<? extends AsyncCallable<? extends T>> stages,
	                                                Collector<T, A, R> collector) {
		A accumulator = collector.supplier().get();
		return reduceSequenceImpl(stages, accumulator, collector);
	}

	/**
	 * @see Stages#reduceSequence(Iterator, Collector)
	 */
	public static <T, A, R> Stage<R> reduceSequence(Stream<? extends AsyncCallable<? extends T>> stages,
	                                                Collector<T, A, R> collector) {
		return reduceSequence(stages.iterator(), collector);
	}

	/**
	 * @see Stages#reduceSequence(Iterator, Collector)
	 */
	public static <T, A, R> Stage<R> reduceSequence(Iterable<? extends AsyncCallable<? extends T>> stages,
	                                                Collector<T, A, R> collector) {
		return reduceSequence(stages.iterator(), collector);
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stage<T> first(AsyncCallable<? extends T>... stages) {
		checkArgument(stages.length != 0);
		return first(asList(stages));
	}

	/**
	 * Predicate in this case picks first {@code Stage} that was completed normally
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	public static <T> Stage<T> first(Iterable<? extends AsyncCallable<? extends T>> stages) {
		return first(stages.iterator());
	}

	/**
     * Predicate in this case picks first {@code Stage} that was completed normally
	 * @see Stages#first(Iterator, BiPredicate)
	 */
	public static <T> Stage<T> first(Iterator<? extends AsyncCallable<? extends T>> stages) {
		return first(stages, (t, throwable) -> throwable == null);
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

	public static <T> Iterator<Stage<T>> iterator(Iterator<AsyncCallable<T>> callables) {
		return new Iterator<Stage<T>>() {
			@Override
			public boolean hasNext() {
				return callables.hasNext();
			}

			@Override
			public Stage<T> next() {
				return callables.next().call();
			}
		};
	}

	public static <T> Iterable<Stage<T>> iterable(Iterable<AsyncCallable<T>> callables) {
		return () -> iterator(callables.iterator());
	}

	public static <T> StageConsumer<T> assertComplete(Consumer<T> consumer) {
		return (t, error) -> {
			if (error != null)
				throw new AssertionError(error);
			consumer.accept(t);
		};
	}

	public static <T> StageConsumer<T> assertComplete() {
		return assertComplete($ -> {});
	}

}
