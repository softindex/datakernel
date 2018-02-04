package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;

public final class Stages {

	public static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException() {
		@Override
		public final Throwable fillInStackTrace() {
			return this;
		}
	};

	private Stages() {
	}

	public static <T> Stage<T> timeout(Stage<T> stage, long timeout) {
		return stage.then(new NextStage<T, T>() {
			final ScheduledRunnable schedule = Eventloop.getCurrentEventloop().delay(timeout,
					() -> tryCompleteExceptionally(TIMEOUT_EXCEPTION));

			@Override
			protected void onComplete(T result) {
				schedule.cancel();
				tryComplete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				schedule.cancel();
				tryCompleteExceptionally(throwable);
			}
		});
	}

	public static <T> BiConsumer<T, Throwable> onError(Consumer<Throwable> consumer) {
		return (t, throwable) -> {
			if (throwable != null) consumer.accept(throwable);
		};
	}

	public static <T> BiConsumer<T, Throwable> onError(Runnable runnable) {
		return (t, throwable) -> {
			if (throwable != null) runnable.run();
		};
	}

	public static <T> BiConsumer<T, Throwable> onResult(Consumer<T> consumer) {
		return (t, throwable) -> {
			if (throwable == null) consumer.accept(t);
		};
	}

	public static <T> BiConsumer<T, Throwable> onResult(Runnable runnable) {
		return (t, throwable) -> {
			if (throwable == null) runnable.run();
		};
	}

	private static final class AllStage<T> extends NextStage<T, Void> {
		int countdown;

		public AllStage(int countdown) {
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

	public static Stage<Void> all(List<? extends Stage<?>> stages) {
		int size = stages.size();
		if (size == 0) return Stage.of(null);
		if (size == 1) return stages.get(0).then(NextStage.toVoid());
		if (size == 2) return stages.get(0).then(NextStage.both(stages.get(1)));
		AllStage<Object> allStage = new AllStage<>(stages.size());
		stages.get(0).then(allStage);
		for (int i = 1; i < size; i++) {
			stages.get(i).then(new NextStage<Object, Object>() {
				@Override
				protected void onComplete(Object result) {
					if (--allStage.countdown == 0) {
						allStage.complete(null);
					}
				}

				@Override
				protected void onCompleteExceptionally(Throwable throwable) {
					allStage.tryCompleteExceptionally(throwable);
				}
			});
		}
		return allStage;
	}

	public static Stage<Void> all(Stream<? extends Stage<Void>> stages) {
		return all(stages.collect(Collectors.toList()));
	}

	public static Stage<Void> all(Stage<?>... stages) {
		return all(asList(stages));
	}

	public static Stage<Void> all(Stage<?> stage1, Stage<?> stage2) {
		return stage1.then(NextStage.both(stage2));
	}

	public static Stage<Void> all(Stage<?> stage) {
		return stage.then(NextStage.toVoid());
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(List<? extends Stage<? extends T>> stages) {
		checkArgument(stages.size() != 0);
		if (stages.size() == 1) return (Stage<T>) stages.get(0);
		if (stages.size() == 2) return stages.get(0).then(NextStage.either(stages.get(1)));
		NextStage<T, T> resultStage = new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				tryComplete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				tryCompleteExceptionally(throwable);
			}
		};
		stages.get(0).then(resultStage);
		for (int i = 1; i < stages.size(); i++) {
			stages.get(i).then(new NextStage<T, Object>() {
				@Override
				protected void onComplete(T result) {
					resultStage.tryComplete(result);
				}

				@Override
				protected void onCompleteExceptionally(Throwable throwable) {
					resultStage.tryCompleteExceptionally(throwable);
				}
			});
		}
		return resultStage;
	}

	@SafeVarargs
	public static <T> Stage<T> any(Stage<? extends T>... stages) {
		return any(asList(stages));
	}

	public static <T> Stage<T> any(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.then(NextStage.either(stage2));
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T> any(Stage<? extends T> stage) {
		return (Stage<T>) stage;
	}

	static final class AnyCompleteStage<T> extends NextStage<T, T> {
		int errors;

		AnyCompleteStage(int errors) {
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

	@SuppressWarnings("unchecked")
	public static <T> Stage<T> anyComplete(List<? extends Stage<? extends T>> stages) {
		int size = stages.size();
		checkArgument(size != 0);
		if (size == 1) return (Stage<T>) stages.get(0);
		if (size == 2) return stages.get(0).then(NextStage.eitherComplete(stages.get(1)));
		AnyCompleteStage<T> resultStage = new AnyCompleteStage<>(size);
		stages.get(0).then(resultStage);
		for (int i = 1; i < size; i++) {
			stages.get(i).then(new NextStage<T, Object>() {
				@Override
				protected void onComplete(T value) {
					resultStage.tryComplete(value);
				}

				@Override
				protected void onCompleteExceptionally(Throwable throwable) {
					if (--resultStage.errors == 0) {
						resultStage.completeExceptionally(throwable);
					}
				}
			});
		}
		return resultStage;
	}

	@SafeVarargs
	public static <T> Stage<T> anyComplete(Stage<? extends T>... stages) {
		return anyComplete(asList(stages));
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T> anyComplete(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.then(NextStage.eitherComplete(stage2));
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T> anyComplete(Stage<? extends T> stage) {
		return (Stage<T>) stage;
	}

	private static final class ReduceStage<T, A, R> extends NextStage<T, R> {
		final IndexedCollector<T, A, R> reducer;
		final A accumulator;
		int stages;

		private ReduceStage(IndexedCollector<T, A, R> reducer, A accumulator, int stages) {
			this.reducer = reducer;
			this.accumulator = accumulator;
			this.stages = stages;
		}

		void processComplete(T result, int i) {
			if (isComplete()) return;
			reducer.accumulate(accumulator, i, result);
			if (--stages == 0) {
				complete(reducer.finish(accumulator));
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

	public static <A, T, R> Stage<R> reduce(List<? extends Stage<? extends T>> stages,
	                                        IndexedCollector<T, A, R> collector) {
		int size = stages.size();
		if (size == 0) return Stage.of(collector.resultOf());
		if (size == 1) return stages.get(0).thenApply(collector::resultOf);
		if (size == 2) return stages.get(0).then(NextStage.combine(stages.get(1), collector::resultOf));

		A accumulator = collector.accumulator(size);
		ReduceStage<T, A, R> resultStage = new ReduceStage<>(collector, accumulator, size);
		stages.get(0).then(resultStage);

		for (int i = 1; i < size; i++) {
			int finalI = i;
			stages.get(i).then(new NextStage<T, Object>() {
				@Override
				protected void onComplete(T result) {
					resultStage.processComplete(result, finalI);
				}

				@Override
				protected void onCompleteExceptionally(Throwable throwable) {
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
		public void onFinish(R result) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
				scheduledRunnable = null;
			}
		}

		@Override
		public void onException(Throwable throwable) {
			if (scheduledRunnable != null) {
				scheduledRunnable.cancel();
				scheduledRunnable = null;
			}
		}

		@Override
		public void run() {
			canceller.cancel();
		}
	}

	private static final class ReduceStageEx<T, A, R> extends NextStage<T, R> implements ReduceListener.ReduceCanceller {
		final IndexedCollector<T, A, R> reducer;
		final ReduceListener<T, A, R> listener;
		final A accumulator;
		int stages;

		private ReduceStageEx(IndexedCollector<T, A, R> reducer, ReduceListener<T, A, R> listener, A accumulator, int stages) {
			this.reducer = reducer;
			this.listener = listener;
			this.accumulator = accumulator;
			this.stages = stages;
		}

		void processComplete(T result, int i) {
			if (isComplete()) return;
			reducer.accumulate(accumulator, i, result);
			if (--stages == 0) {
				R finish = reducer.finish(accumulator);
				listener.onFinish(finish);
				complete(finish);
			}
		}

		@Override
		protected void onComplete(T result) {
			processComplete(result, 0);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			if (isComplete()) return;
			listener.onException(throwable);
			completeExceptionally(throwable);
		}

		@Override
		public void cancel() {
			if (isComplete()) return;
			R finish = reducer.finish(accumulator);
			listener.onFinish(finish);
			complete(finish);
		}

		@Override
		public void cancel(Throwable throwable) {
			tryCompleteExceptionally(throwable);
		}
	}

	public static <A, T, R> Stage<R> reduce(List<? extends Stage<? extends T>> stages, ReduceListener<T, A, R> listener,
	                                        IndexedCollector<T, A, R> collector) {
		int size = stages.size();
		if (size == 0) {
			R finished = collector.resultOf();
			listener.onFinish(finished);
			return Stage.of(finished);
		}

		A accumulator = collector.accumulator(size);
		ReduceStageEx<T, A, R> resultStage = new ReduceStageEx<>(collector, listener, accumulator, size);
		listener.onStart(resultStage, accumulator);
		stages.get(0).then(resultStage);

		for (int i = 1; i < size; i++) {
			int finalI = i;
			stages.get(i).then(new NextStage<T, Object>() {
				@Override
				protected void onComplete(T result) {
					resultStage.processComplete(result, finalI);
				}

				@Override
				protected void onCompleteExceptionally(Throwable throwable) {
					resultStage.onCompleteExceptionally(throwable);
				}
			});
		}
		return resultStage;
	}

	public static <T> Stage<List<T>> reduceToList(List<? extends Stage<? extends T>> stages) {
		return reduce(stages, IndexedCollector.toList());
	}

	public static <T> Stage<List<T>> reduceToList(Stream<? extends Stage<? extends T>> stages) {
		List<? extends Stage<? extends T>> list = stages.collect(Collectors.toList());
		return reduceToList(list);
	}

	@SafeVarargs
	public static <T> Stage<List<T>> reduceToList(Stage<? extends T>... stages) {
		return reduceToList(asList(stages));
	}

	public static <T> Stage<List<T>> reduceToList(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.then(NextStage.combine(stage2, (value1, value2) -> asList(value1, value2)));
	}

	public static <T> Stage<List<T>> reduceToList(Stage<? extends T> stage1) {
		return stage1.thenApply(Collections::singletonList);
	}

	public static <T> Stage<T[]> reduceToArray(List<? extends Stage<? extends T>> stages) {
		return reduce(stages, IndexedCollector.toArray());
	}

	public static <T> Stage<T[]> reduceToArray(Stream<? extends Stage<? extends T>> stages) {
		List<? extends Stage<? extends T>> list = stages.collect(Collectors.toList());
		return reduceToArray(list);
	}

	@SafeVarargs
	public static <T> Stage<T[]> reduceToArray(Stage<? extends T>... stages) {
		return reduceToArray(asList(stages));
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T[]> reduceToArray(Stage<? extends T> stage1, Stage<? extends T> stage2) {
		return stage1.then(NextStage.combine(stage2, (value1, value2) -> (T[]) new Object[]{value1, value2}));
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T[]> reduceToArray(Stage<? extends T> stage1) {
		return stage1.thenApply(value -> (T[]) new Object[]{value});
	}

	public static Stage<Void> runSequence(Iterator<? extends AsyncCallable<?>> stages) {
		if (!stages.hasNext()) {
			return Stage.of(null);
		}
		return stages.next().call().thenComposeAsync(value -> runSequence(stages));
	}

	public static Stage<Void> runSequence(Iterable<? extends AsyncCallable<?>> stages) {
		return runSequence(stages.iterator());
	}

	public static Stage<Void> runSequence(Stream<? extends AsyncCallable<?>> stages) {
		return runSequence(stages.iterator());
	}

	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable... stages) {
		return runSequence((AsyncCallable) asList(stages));
	}

	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable stage) {
		return stage.call().then(NextStage.toVoid());
	}

	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable stage1, AsyncCallable stage2) {
		return stage1.call().thenCompose($ -> runSequence(stage2));
	}

	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable stage1, AsyncCallable stage2, AsyncCallable stage3) {
		return stage1.call().thenCompose($ -> runSequence(stage2, stage3));
	}

	private static <T, A, R> Stage<R> reduceSequenceImpl(Iterator<? extends AsyncCallable<? extends T>> stages, A accumulator,
	                                                     Collector<T, A, R> collector) {
		if (!stages.hasNext()) {
			return Stage.of(collector.finisher().apply(accumulator));
		}
		return stages.next().call().thenComposeAsync(value -> {
			collector.accumulator().accept(accumulator, value);
			return reduceSequenceImpl(stages, accumulator, collector);
		});
	}

	public static <T, A, R> Stage<R> reduceSequence(Iterator<? extends AsyncCallable<? extends T>> stages,
	                                                Collector<T, A, R> collector) {
		A accumulator = collector.supplier().get();
		return reduceSequenceImpl(stages, accumulator, collector);
	}

	public static <T, A, R> Stage<R> reduceSequence(Stream<? extends AsyncCallable<? extends T>> stages,
	                                                Collector<T, A, R> collector) {
		return reduceSequence(stages.iterator(), collector);
	}

	public static <T, A, R> Stage<R> reduceSequence(Iterable<? extends AsyncCallable<? extends T>> stages,
	                                                Collector<T, A, R> collector) {
		return reduceSequence(stages.iterator(), collector);
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T> firstComplete(AsyncCallable<? extends T>... stages) {
		checkArgument(stages.length != 0);
		return firstComplete(asList(stages));
	}

	public static <T> Stage<T> firstComplete(Iterable<? extends AsyncCallable<? extends T>> stages) {
		return firstComplete(stages.iterator());
	}

	public static <T> Stage<T> firstComplete(Iterator<? extends AsyncCallable<? extends T>> stages) {
		return first(stages, (t, throwable) -> throwable == null);
	}

	public static <T> Stage<T> first(Iterable<? extends AsyncCallable<? extends T>> stages,
	                                 Predicate<? super T> predicate) {
		return first(stages, (result, throwable) -> (throwable != null) || predicate.test(result));
	}

	public static <T> Stage<T> first(Iterator<? extends AsyncCallable<? extends T>> stages,
	                                 Predicate<? super T> predicate) {
		return first(stages, (result, throwable) -> (throwable != null) || predicate.test(result));
	}

	public static <T> Stage<T> first(Iterable<? extends AsyncCallable<? extends T>> stages,
	                                 BiPredicate<? super T, ? super Throwable> predicate) {
		return first(stages.iterator(), predicate);
	}

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
		callable.call().whenCompleteAsync((result, throwable) -> {
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

	public static <T> BiConsumer<T, ? super Throwable> assertComplete(Consumer<T> consumer) {
		return (BiConsumer<T, Throwable>) (t, throwable) -> {
			if (throwable != null)
				throw new AssertionError(throwable);
			consumer.accept(t);
		};
	}

	public static <T> BiConsumer<T, ? super Throwable> assertComplete() {
		return assertComplete($ -> {});
	}

}
