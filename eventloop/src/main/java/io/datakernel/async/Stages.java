package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public final class Stages {

	private Stages() {
	}

	public static <T> CompletionStage<T> of(T value) {
		SettableStage<T> stage = new SettableStage<>();
		stage.supplier = () -> value;
		return stage;
	}

	public static <T> CompletionStage<T> ofSupplier(Supplier<T> supplier) {
		SettableStage<T> stage = new SettableStage<>();
		stage.supplier = supplier;
		return stage;
	}

	public static <T> CompletionStage<T> ofException(Throwable throwable) {
		SettableStage<T> stage = new SettableStage<>();
		stage.supplier = null;
		stage.exception = throwable;
		return stage;
	}

	public static <T> CompletionStage<T> timeout(Eventloop eventloop, CompletionStage<T> stage, long timestampMillis) {
		SettableStage<T> resultStage = SettableStage.create();
		ScheduledRunnable schedule = eventloop.schedule(timestampMillis, () -> resultStage.setException(new TimeoutException()));
		stage.whenComplete((result, throwable) -> {
			schedule.cancel();
			if (!resultStage.isComplete()) {
				if (throwable == null) {
					resultStage.set(result);
				} else {
					resultStage.setException(throwable);
				}
			}
		});
		return resultStage;
	}

	public static <T> BiConsumer<T, Throwable> onError(Consumer<Throwable> consumer) {
		return (t, throwable) -> {
			if (throwable != null) consumer.accept(throwable);
		};
	}

	public static <T> BiConsumer<T, Throwable> onResult(Consumer<T> consumer) {
		return (t, throwable) -> {
			if (throwable == null) consumer.accept(t);
		};
	}

	public static <T> CompletionStage<T> ofFuture(CompletableFuture<T> completableFuture) {
		return ofFuture(Eventloop.getCurrentEventloop(), completableFuture);
	}

	public static <T> CompletionStage<T> ofFuture(Eventloop eventloop, CompletableFuture<T> completableFuture) {
		final SettableStage<T> stage = SettableStage.create();
		completableFuture.whenComplete((t, throwable) -> eventloop.execute(() -> stage.set(t, throwable)));
		return stage;
	}

	public static <T> CompletionStage<T> ofFuture(Future<T> future, Executor executor) {
		return ofFuture(Eventloop.getCurrentEventloop(), future, executor);
	}

	public static <T> CompletionStage<T> ofFuture(Eventloop eventloop, Future<T> future, Executor executor) {
		final SettableStage<T> stage = SettableStage.create();
		executor.execute(() -> {
			try {
				final T value = future.get();
				eventloop.execute(() -> stage.set(value));
			} catch (InterruptedException | ExecutionException e) {
				eventloop.execute(() -> stage.setException(e));
			}
		});
		return stage;
	}

	private static final class Counter {
		int counter;

		public Counter(int counter) {
			this.counter = counter;
		}
	}

	public static <T> AsyncCallable<T> callable(Supplier<CompletionStage<T>> function) {
		return function::get;
	}

	public static <T, A> Function<A, AsyncCallable<T>> callable(Function<A, CompletionStage<T>> function) {
		return a -> () -> function.apply(a);
	}

	public static CompletionStage<Void> run(AsyncCallable<?>... stages) {
		return runCallable(asList(stages));
	}

	public static CompletionStage<Void> run(CompletionStage<?>... stages) {
		return run(asList(stages));
	}

	public static CompletionStage<Void> runCallable(List<? extends AsyncCallable<?>> stages) {
		return reduceCallable(null, (accumulator, value, index) -> {
		}, stages);
	}

	public static CompletionStage<Void> run(List<? extends CompletionStage<?>> stages) {
		return reduce(null, (accumulator, value, index) -> {
		}, stages);
	}

	@SafeVarargs
	public static <T> CompletionStage<List<T>> collect(AsyncCallable<? extends T>... stages) {
		return collectCallable(asList(stages));
	}

	@SafeVarargs
	public static <T> CompletionStage<List<T>> collect(CompletionStage<? extends T>... stages) {
		return collect(asList(stages));
	}

	@SuppressWarnings("unchecked")
	public static <T> CompletionStage<List<T>> collectCallable(List<? extends AsyncCallable<? extends T>> stages) {
		return reduceCallable(asList((T[]) new Object[stages.size()]), (accumulator, value, index) -> accumulator.set(index, value), stages);
	}

	@SuppressWarnings("unchecked")
	public static <T> CompletionStage<List<T>> collect(List<? extends CompletionStage<? extends T>> stages) {
		return reduce(asList((T[]) new Object[stages.size()]), (accumulator, value, index) -> accumulator.set(index, value), stages);
	}

	public static <A, T> CompletionStage<A> reduceCallable(A accumulator, IndexedReducer<A, T> reducer,
	                                                       AsyncCallable<? extends T>... stages) {
		return reduceCallable(accumulator, reducer, asList(stages));
	}

	@SafeVarargs
	public static <A, T> CompletionStage<A> reduce(A accumulator, IndexedReducer<A, T> reducer,
	                                               CompletionStage<? extends T>... stages) {
		return reduce(accumulator, reducer, asList(stages));
	}

	public static <A, T> CompletionStage<A> reduceCallable(A accumulator, IndexedReducer<A, T> reducer,
	                                                       List<? extends AsyncCallable<? extends T>> stages) {
		return reduce(accumulator, reducer, stages.stream().map(AsyncCallable::call).collect(Collectors.toList()));
	}

	public static <A, T> CompletionStage<A> reduce(A accumulator, IndexedReducer<A, T> reducer,
	                                               List<? extends CompletionStage<? extends T>> stages) {
		if (stages.size() == 0) {
			return of(accumulator);
		}
		if (stages.size() == 1) {
			return stages.get(0).thenApply(value -> {
				reducer.accumulate(accumulator, value, 0);
				return accumulator;
			});
		}
		SettableStage<A> resultStage = SettableStage.create();
		Counter counter = new Counter(stages.size());
		for (int i = 0; i < stages.size(); i++) {
			int finalI = i;
			stages.get(i).whenComplete((result, throwable) -> {
				if (throwable == null) {
					if (!resultStage.isSet()) {
						reducer.accumulate(accumulator, result, finalI);
						if (--counter.counter == 0) {
							resultStage.set(accumulator);
						}
					}
				} else {
					resultStage.trySetException(throwable);
				}
			});
		}
		return resultStage;
	}

	public static CompletionStage<Void> runSequence(AsyncCallable<?>... stages) {
		return runSequence(asList(stages));
	}

	public static CompletionStage<Void> runSequence(Iterable<? extends AsyncCallable<?>> stages) {
		return reduceSequence(null, (accumulator, value, index) -> {
		}, stages.iterator());
	}

	public static CompletionStage<Void> runSequence(Iterator<? extends AsyncCallable<?>> stages) {
		return reduceSequence(null, (accumulator, value, index) -> {
		}, stages);
	}

	@SafeVarargs
	public static <T> CompletionStage<List<T>> collectSequence(AsyncCallable<? extends T>... stages) {
		return collectSequence(asList(stages));
	}

	public static <T> CompletionStage<List<T>> collectSequence(Iterable<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequence(new ArrayList<>(), (accumulator, value, index) -> accumulator.add(value), stages);
	}

	public static <T> CompletionStage<List<T>> collectSequence(Iterator<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequence(new ArrayList<>(), (accumulator, value, index) -> accumulator.add(value), stages);
	}

	@SafeVarargs
	public static <T, A> CompletionStage<A> reduceSequence(A accumulator, IndexedReducer<A, T> reducer,
	                                                       AsyncCallable<? extends T>... stages) {
		return reduceSequence(accumulator, reducer, asList(stages));
	}

	public static <T, A> CompletionStage<A> reduceSequence(A accumulator, IndexedReducer<A, T> reducer,
	                                                       Iterable<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequence(accumulator, reducer, stages.iterator());
	}

	public static <T, A> CompletionStage<A> reduceSequence(A accumulator, IndexedReducer<A, T> reducer,
	                                                       Iterator<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequenceImpl(accumulator, reducer, 0, stages);
	}

	private static <T, A> CompletionStage<A> reduceSequenceImpl(A accumulator, IndexedReducer<A, T> reducer, int index,
	                                                            Iterator<? extends AsyncCallable<? extends T>> stages) {
		if (!stages.hasNext()) {
			return of(accumulator);
		}
		return stages.next().call().thenComposeAsync(value -> {
			reducer.accumulate(accumulator, value, index);
			return reduceSequenceImpl(accumulator, reducer, index + 1, stages);
		});
	}

	public static <T> Iterator<CompletionStage<T>> iterator(Iterator<AsyncCallable<T>> callables) {
		return new Iterator<CompletionStage<T>>() {
			@Override
			public boolean hasNext() {
				return callables.hasNext();
			}

			@Override
			public CompletionStage<T> next() {
				return callables.next().call();
			}
		};
	}

	public static <T> Iterable<CompletionStage<T>> iterable(Iterable<AsyncCallable<T>> callables) {
		return () -> iterator(callables.iterator());
	}

	public static final class Pair<L, R> {
		private int counter = 2;
		private L left;
		private R right;

		private Pair() {
		}

		public L getLeft() {
			return left;
		}

		public R getRight() {
			return right;
		}
	}

	@SuppressWarnings("unchecked")
	public static <L, R> CompletionStage<Pair<L, R>> pair(CompletionStage<L> left, CompletionStage<R> right) {
		SettableStage<Pair<L, R>> resultStage = SettableStage.create();
		Pair<L, R> results = new Pair<>();
		left.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (!resultStage.isSet()) {
					results.left = result;
					if (--results.counter == 0) {
						resultStage.set(results);
					}
				}
			} else {
				results.left = null;
				results.right = null;
				resultStage.trySetException(throwable);
			}
		});
		right.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (!resultStage.isSet()) {
					results.right = result;
					if (--results.counter == 0) {
						resultStage.set(results);
					}
				}
			} else {
				results.left = null;
				results.right = null;
				resultStage.trySetException(throwable);
			}
		});
		return resultStage;
	}

	public static <T> BiConsumer<T, ? super Throwable> assertComplete(Consumer<T> consumer) {
		return (BiConsumer<T, Throwable>) (t, throwable) -> {
			if (throwable != null)
				throw new AssertionError(throwable);
			consumer.accept(t);
		};
	}

}
