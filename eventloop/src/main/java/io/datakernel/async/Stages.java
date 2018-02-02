package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public final class Stages {

	private Stages() {
	}

	public static <T> Stage<T> timeout(Stage<T> stage, long timestamp) {
		SettableStage<T> resultStage = SettableStage.create();
		ScheduledRunnable schedule = Eventloop.getCurrentEventloop()
				.schedule(timestamp, () -> resultStage.setException(new TimeoutException()));
		stage.whenComplete((result, throwable) -> {
			schedule.cancel();
			if (!resultStage.isComplete()) {
				resultStage.set(result, throwable);
			}
		});
		return resultStage;
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

	private static final class Counter {
		int counter;

		public Counter(int counter) {
			this.counter = counter;
		}
	}

	public static Stage<Void> run(Stage<Void>... stages) {
		return run(asList(stages));
	}

	public static Stage<Void> run(Stream<? extends Stage<Void>> stages) {
		return run(stages.collect(toList()));
	}

	public static Stage<Void> run(List<? extends Stage<Void>> stages) {
		return reduce(null, (accumulator, value, index) -> {}, stages);
	}

	@SafeVarargs
	public static <T> Stage<List<T>> collect(Stage<? extends T>... stages) {
		return collect(asList(stages));
	}

	public static <T> Stage<List<T>> collect(Stream<? extends Stage<? extends T>> stages) {
		List<? extends Stage<? extends T>> list = stages.collect(toList());
		return collect(list);
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<List<T>> collect(List<? extends Stage<? extends T>> stages) {
		return reduce(asList((T[]) new Object[stages.size()]), (accumulator, value, index) -> accumulator.set(index, value), stages);
	}

	@SafeVarargs
	public static <A, T> Stage<A> reduce(A accumulator, IndexedReducer<A, T> reducer,
	                                     Stage<? extends T>... stages) {
		return reduce(accumulator, reducer, asList(stages));
	}

	public static <A, T> Stage<A> reduce(A accumulator, IndexedReducer<A, T> reducer,
	                                     Stream<? extends Stage<? extends T>> stages) {
		return reduce(accumulator, reducer, stages.collect(toList()));
	}

	public static <A, T> Stage<A> reduce(A accumulator, IndexedReducer<A, T> reducer,
	                                     List<? extends Stage<? extends T>> stages) {
		if (stages.size() == 0) {
			return Stage.of(accumulator);
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

	@SuppressWarnings("unchecked")
	public static Stage<Void> runSequence(AsyncCallable<Void>... stages) {
		return runSequence(asList(stages));
	}

	public static Stage<Void> runSequence(Stream<? extends AsyncCallable<Void>> stages) {
		return runSequence(stages.collect(toList()));
	}

	public static Stage<Void> runSequence(Iterable<? extends AsyncCallable<Void>> stages) {
		return reduceSequence(null, (accumulator, value, index) -> {}, stages.iterator());
	}

	public static Stage<Void> runSequence(Iterator<? extends AsyncCallable<Void>> stages) {
		return reduceSequence(null, (accumulator, value, index) -> {}, stages);
	}

	@SafeVarargs
	public static <T> Stage<List<T>> collectSequence(AsyncCallable<? extends T>... stages) {
		return collectSequence(asList(stages));
	}

	public static <T> Stage<List<T>> collectSequence(Stream<? extends AsyncCallable<? extends T>> stages) {
		List<? extends AsyncCallable<? extends T>> list = stages.collect(toList());
		return collectSequence(list);
	}

	public static <T> Stage<List<T>> collectSequence(Iterable<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequence(new ArrayList<>(), (accumulator, value, index) -> accumulator.add(value), stages);
	}

	public static <T> Stage<List<T>> collectSequence(Iterator<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequence(new ArrayList<>(), (accumulator, value, index) -> accumulator.add(value), stages);
	}

	@SafeVarargs
	public static <T, A> Stage<A> reduceSequence(A accumulator, IndexedReducer<A, T> reducer,
	                                             AsyncCallable<? extends T>... stages) {
		return reduceSequence(accumulator, reducer, asList(stages));
	}

	public static <T, A> Stage<A> reduceSequence(A accumulator, IndexedReducer<A, T> reducer,
	                                             Stream<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequence(accumulator, reducer, stages.collect(toList()));
	}

	public static <T, A> Stage<A> reduceSequence(A accumulator, IndexedReducer<A, T> reducer,
	                                             Iterable<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequence(accumulator, reducer, stages.iterator());
	}

	public static <T, A> Stage<A> reduceSequence(A accumulator, IndexedReducer<A, T> reducer,
	                                             Iterator<? extends AsyncCallable<? extends T>> stages) {
		return reduceSequenceImpl(accumulator, reducer, 0, stages);
	}

	private static <T, A> Stage<A> reduceSequenceImpl(A accumulator, IndexedReducer<A, T> reducer, int index,
	                                                  Iterator<? extends AsyncCallable<? extends T>> stages) {
		if (!stages.hasNext()) {
			return Stage.of(accumulator);
		}
		return stages.next().call().thenComposeAsync(value -> {
			reducer.accumulate(accumulator, value, index);
			return reduceSequenceImpl(accumulator, reducer, index + 1, stages);
		});
	}

	public static <T> Stage<T> first(List<? extends Stage<? extends T>> stages) {
		SettableStage<T> result = SettableStage.create();
		for (Stage<? extends T> stage : stages) {
			stage.whenComplete(result::trySet);
		}
		return result;
	}

	@SafeVarargs
	public static <T> Stage<T> first(Stage<? extends T>... stages) {
		return first(Arrays.asList(stages));
	}

	public static <T> Stage<T> firstComplete(List<? extends Stage<? extends T>> stages) {
		SettableStage<T> result = SettableStage.create();
		int[] countdown = new int[]{stages.size()};
		for (Stage<? extends T> stage : stages) {
			stage.whenComplete((value, throwable) -> {
				if (throwable == null) {
					result.trySet(value);
				} else {
					if (--countdown[0] == 0) {
						result.setException(throwable);
					}
				}
			});
		}
		return result;
	}

	@SafeVarargs
	public static <T> Stage<T> firstComplete(Stage<? extends T>... stages) {
		return firstComplete(Arrays.asList(stages));
	}

	@SuppressWarnings("unchecked")
	public static <T> Stage<T> firstComplete(AsyncCallable<? extends T>... stages) {
		return firstComplete(Arrays.asList(stages));
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
