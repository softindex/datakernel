package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.Eventloop.ConcurrentOperationTracker;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

abstract class AbstractCompletionStage<T> implements CompletionStage<T> {
	protected static abstract class NextCompletionStage<F, T> extends AbstractCompletionStage<T> {

		protected abstract void onComplete(F value);

		protected void onCompleteExceptionally(Throwable error) {
			completeExceptionally(error);
		}
	}

	private static final NextCompletionStage COMPLETED_STAGE = new CompletedStage();

	protected NextCompletionStage<T, ?> next;

	protected boolean isComplete() {
		return next == COMPLETED_STAGE;
	}

	protected final void complete(T value) {
		assert !isComplete();
		if (next != null) {
			next.onComplete(value);
			next = COMPLETED_STAGE;
		}
	}

	protected void completeExceptionally(Throwable error) {
		assert !isComplete();
		if (next != null) {
			next.onCompleteExceptionally(error);
			next = COMPLETED_STAGE;
		}
	}

	private static final class SplittingStage<T> extends NextCompletionStage<T, T> {
		private final ArrayList<NextCompletionStage<T, ?>> list = new ArrayList<>();

		private static <T> SplittingStage<T> of(NextCompletionStage<T, ?> existing, NextCompletionStage<T, ?> next) {
			SplittingStage<T> stage = new SplittingStage<>();
			stage.list.add(existing);
			stage.list.add(next);
			return stage;
		}

		@Override
		protected void onComplete(T value) {
			for (NextCompletionStage<T, ?> next : list) {
				next.onComplete(value);
			}
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			for (NextCompletionStage<T, ?> next : list) {
				next.onCompleteExceptionally(error);
			}
		}
	}

	protected <X> CompletionStage<X> subscribe(NextCompletionStage<T, X> next) {
		if (this.next == null) {
			this.next = next;
		} else {
			if (this.next == COMPLETED_STAGE)
				throw new IllegalStateException();
			if (this.next instanceof SplittingStage) {
				((SplittingStage<T>) this.next).list.add(next);
			} else {
				this.next = SplittingStage.of(this.next, next);
			}
		}
		return next;
	}

	@Override
	public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				complete(fn.apply(value));
			}
		});
	}

	@Override
	public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
		//noinspection unchecked
		return ((NextCompletionStage<U, U>) thenApply(fn)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T result) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					final U apply = fn.apply(result);
					eventloop.execute(() -> {
						complete(apply);
						tracker.complete();
					});
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	@Override
	public CompletionStage<Void> thenAccept(Consumer<? super T> action) {
		return subscribe(new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T value) {
				action.accept(value);
				complete(null);
			}
		});
	}

	@Override
	public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action) {
		//noinspection unchecked
		return ((NextCompletionStage<Void, Void>) thenAccept(action)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		return subscribe(new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T result) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					action.accept(result);
					eventloop.execute(() -> {
						complete(null);
						tracker.complete();
					});
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	@Override
	public CompletionStage<Void> thenRun(Runnable action) {
		return subscribe(new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T value) {
				action.run();
				complete(null);
			}
		});
	}

	@Override
	public CompletionStage<Void> thenRunAsync(Runnable action) {
		//noinspection unchecked
		return ((NextCompletionStage<Void, Void>) thenRun(action)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		return subscribe(new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T result) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					action.run();
					eventloop.execute(() -> {
						complete(null);
						tracker.complete();
					});
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	private static class ThenCombineStage<T, V, U> extends NextCompletionStage<T, V> {
		private final BiFunction<? super T, ? super U, ? extends V> fn;
		private int countdown = 2;
		private T thisResult;
		private U otherResult;

		ThenCombineStage(BiFunction<? super T, ? super U, ? extends V> fn) {
			this.fn = fn;
		}

		private void tryComplete() {
			if (--countdown == 0) {
				complete(fn.apply(thisResult, otherResult));
				thisResult = null;
				otherResult = null;
			}
		}

		@Override
		protected void onComplete(T value) {
			thisResult = value;
			tryComplete();
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			countdown = 0;
			completeExceptionally(error);
		}
	}

	@Override
	public <U, V> CompletionStage<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		ThenCombineStage<T, V, U> resultingStage = new ThenCombineStage<>(fn);

		other.whenComplete((BiConsumer<U, Throwable>) (result, throwable) -> {
			if (throwable == null) {
				resultingStage.otherResult = result;
				resultingStage.tryComplete();
			} else {
				resultingStage.onCompleteExceptionally(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		//noinspection unchecked
		return ((NextCompletionStage<V, V>) thenCombine(other, fn)).subscribe(new PostNextCompletionStage());
	}

	private static class ConcurrentThenCombineStage<T, V, U> extends NextCompletionStage<T, V> {
		private final BiFunction<? super T, ? super U, ? extends V> fn;
		private final Executor executor;
		private final Eventloop eventloop;
		private int countdown = 2;
		private T thisResult;
		private U otherResult;

		ConcurrentThenCombineStage(Eventloop eventloop, Executor executor, BiFunction<? super T, ? super U, ? extends V> fn) {
			this.eventloop = eventloop;
			this.executor = executor;
			this.fn = fn;
		}

		private void tryComplete() {
			if (--countdown == 0) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					final V apply = fn.apply(thisResult, otherResult);
					eventloop.execute(() -> {
						thisResult = null;
						otherResult = null;
						complete(apply);
						tracker.complete();
					});
				});
			}
		}

		@Override
		protected void onComplete(T result) {
			thisResult = result;
			tryComplete();
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			countdown = 0;
			eventloop.post(() -> completeExceptionally(error));
		}
	}

	@Override
	public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
		ConcurrentThenCombineStage<T, V, U> resultingStage = new ConcurrentThenCombineStage<>(getCurrentEventloop(), executor, fn);

		other.whenComplete((BiConsumer<U, Throwable>) (result, throwable) -> {
			if (throwable == null) {
				resultingStage.otherResult = result;
				resultingStage.tryComplete();
			} else {
				resultingStage.onCompleteExceptionally(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	private static final class ThenAcceptBothStage<T, U> extends NextCompletionStage<T, Void> {
		private final BiConsumer<? super T, ? super U> consumer;
		private int countdown = 2;
		private T thisValue;
		private U otherValue;

		private ThenAcceptBothStage(BiConsumer<? super T, ? super U> consumer) {
			this.consumer = consumer;
		}

		private void tryComplete() {
			if (--countdown == 0) {
				consumer.accept(thisValue, otherValue);
				thisValue = null;
				otherValue = null;
				complete(null);
			}
		}

		@Override
		protected void onComplete(T value) {
			thisValue = value;
			tryComplete();
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			if (!isComplete()) {
				countdown = 0;
				completeExceptionally(error);
			}
		}
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
		ThenAcceptBothStage<T, U> resultingStage = new ThenAcceptBothStage<>(action);

		other.whenComplete((result, throwable) -> {
			if (throwable == null) {
				resultingStage.otherValue = result;
				resultingStage.tryComplete();
			} else {
				resultingStage.onCompleteExceptionally(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
		//noinspection unchecked
		return ((NextCompletionStage<Void, Void>) thenAcceptBoth(other, action)).subscribe(new PostNextCompletionStage());
	}

	private static final class ConcurrentThenAcceptBothStage<T, U> extends NextCompletionStage<T, Void> {
		private final BiConsumer<? super T, ? super U> consumer;
		private final Eventloop eventloop;
		private final Executor executor;
		private int countdown = 2;
		private T thisResult;
		private U otherResult;

		private ConcurrentThenAcceptBothStage(Eventloop eventloop, Executor executor, BiConsumer<? super T, ? super U> consumer) {
			this.eventloop = eventloop;
			this.executor = executor;
			this.consumer = consumer;
		}

		private void tryComplete() {
			if (--countdown == 0) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					consumer.accept(thisResult, otherResult);
					eventloop.execute(() -> {
						thisResult = null;
						otherResult = null;
						complete(null);
						tracker.complete();
					});
				});
			}
		}

		@Override
		protected void onComplete(T result) {
			thisResult = result;
			tryComplete();
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			if (!isComplete()) {
				countdown = 0;
				eventloop.post(() -> completeExceptionally(error));
			}
		}
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
		ConcurrentThenAcceptBothStage<T, U> resultingStage = new ConcurrentThenAcceptBothStage<>(getCurrentEventloop(), executor, action);

		other.whenComplete((result, throwable) -> {
			if (throwable == null) {
				resultingStage.otherResult = result;
				resultingStage.tryComplete();
			} else {
				resultingStage.onCompleteExceptionally(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	private static final class RunAfterBothStage<T> extends NextCompletionStage<T, Void> {
		private final Runnable action;
		private int countdown = 2;

		private RunAfterBothStage(Runnable action) {
			this.action = action;
		}

		@Override
		protected void onComplete(T value) {
			if (--countdown == 0) {
				action.run();
				complete(null);
			}
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			if (!isComplete()) {
				countdown = 0;
				completeExceptionally(error);
			}
		}
	}

	@Override
	public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
		RunAfterBothStage<T> resultingStage = new RunAfterBothStage<>(action);

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				resultingStage.onComplete(null);
			} else {
				resultingStage.onCompleteExceptionally(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
		//noinspection unchecked
		return ((NextCompletionStage<Void, Void>) runAfterBoth(other, action)).subscribe(new PostNextCompletionStage());
	}

	private static final class ConcurrentRunAfterBothStage<T> extends NextCompletionStage<T, Void> {
		private final Eventloop eventloop;
		private final Executor executor;
		private final Runnable action;
		private int countdown = 2;

		private ConcurrentRunAfterBothStage(Eventloop eventloop, Executor executor, Runnable action) {
			this.eventloop = eventloop;
			this.executor = executor;
			this.action = action;
		}

		@Override
		protected void onComplete(T result) {
			if (--countdown == 0) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					action.run();
					eventloop.execute(() -> {
						complete(null);
						tracker.complete();
					});
				});
			}
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			if (!isComplete()) {
				countdown = 0;
				eventloop.post(() -> completeExceptionally(error));
			}
		}
	}

	@Override
	public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
		ConcurrentRunAfterBothStage<T> resultingStage = new ConcurrentRunAfterBothStage<>(getCurrentEventloop(), executor, action);

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				resultingStage.onComplete(null);
			} else {
				resultingStage.onCompleteExceptionally(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
		NextCompletionStage<T, U> resultingStage = new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				if (!isComplete()) {
					complete(fn.apply(value));
				}
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				if (!isComplete()) {
					completeExceptionally(error);
				}
			}
		};

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				if (!resultingStage.isComplete()) {
					resultingStage.complete(fn.apply(o));
				}
			} else {
				if (!resultingStage.isComplete()) {
					resultingStage.completeExceptionally(throwable);
				}
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
		//noinspection unchecked
		return ((NextCompletionStage<U, U>) applyToEither(other, fn)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		NextCompletionStage<T, U> resultingStage = new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T result) {
				if (!isComplete()) {
					final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
					executor.execute(() -> {
						final U apply = fn.apply(result);
						eventloop.execute(() -> {
							if (!isComplete()) complete(apply);
							tracker.complete();
						});
					});
				}
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				if (!isComplete()) {
					eventloop.post(() -> completeExceptionally(error));
				}
			}
		};

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				if (!resultingStage.isComplete()) {
					final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
					executor.execute(() -> {
						final U apply = fn.apply(o);
						eventloop.execute(() -> {
							if (!resultingStage.isComplete()) resultingStage.complete(apply);
							tracker.complete();
						});
					});
				}
			} else {
				if (!resultingStage.isComplete()) {
					eventloop.post(() -> resultingStage.completeExceptionally(throwable));
				}
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
		NextCompletionStage<T, Void> resultingStage = new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T value) {
				if (!isComplete()) {
					action.accept(value);
					complete(null);
				}
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				if (!isComplete()) {
					completeExceptionally(error);
				}
			}
		};

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				if (!resultingStage.isComplete()) {
					action.accept(o);
					resultingStage.complete(null);
				}
			} else {
				if (!resultingStage.isComplete()) {
					resultingStage.completeExceptionally(throwable);
				}
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
		//noinspection unchecked
		return ((NextCompletionStage<Void, Void>) acceptEither(other, action)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		NextCompletionStage<T, Void> resultingStage = new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T result) {
				if (!isComplete()) {
					final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
					executor.execute(() -> {
						action.accept(result);
						eventloop.execute(() -> {
							if (!isComplete()) complete(null);
							tracker.complete();
						});
					});
				}
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				if (!isComplete()) {
					eventloop.post(() -> completeExceptionally(error));
				}
			}
		};

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				if (!resultingStage.isComplete()) {
					final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
					executor.execute(() -> {
						action.accept(o);
						eventloop.execute(() -> {
							if (!resultingStage.isComplete()) resultingStage.complete(null);
							tracker.complete();
						});
					});
				}
			} else {
				if (!resultingStage.isComplete()) {
					eventloop.post(() -> resultingStage.completeExceptionally(throwable));
				}
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
		NextCompletionStage<T, Void> resultingStage = new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T value) {
				if (!isComplete()) {
					action.run();
					complete(null);
				}
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				if (!isComplete()) {
					completeExceptionally(error);
				}
			}
		};

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				if (!resultingStage.isComplete()) {
					action.run();
					resultingStage.complete(null);
				}
			} else {
				if (!resultingStage.isComplete()) {
					resultingStage.completeExceptionally(throwable);
				}
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
		//noinspection unchecked
		return ((NextCompletionStage<Void, Void>) runAfterEither(other, action)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		NextCompletionStage<T, Void> resultingStage = new NextCompletionStage<T, Void>() {
			@Override
			protected void onComplete(T result) {
				if (!isComplete()) {
					final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
					executor.execute(() -> {
						action.run();
						eventloop.execute(() -> {
							if (!isComplete()) complete(null);
							tracker.complete();
						});
					});
				}
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				if (!isComplete()) {
					eventloop.post(() -> completeExceptionally(error));
				}
			}
		};

		other.whenComplete((o, throwable) -> {
			if (throwable == null) {
				if (!resultingStage.isComplete()) {
					final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
					executor.execute(() -> {
						action.run();
						eventloop.execute(() -> {
							if (!resultingStage.isComplete()) resultingStage.complete(null);
							tracker.complete();
						});
					});
				}
			} else {
				if (!resultingStage.isComplete()) {
					eventloop.post(() -> resultingStage.completeExceptionally(throwable));
				}
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				fn.apply(value)
						.whenComplete((u, throwable) -> {
							if (throwable != null) {
								completeExceptionally(throwable);
							} else {
								complete(u);
							}
						});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				completeExceptionally(error);
			}
		});
	}

	@Override
	public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
		//noinspection unchecked
		return ((NextCompletionStage<U, U>) thenCompose(fn)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T result) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					final CompletionStage<U> apply = fn.apply(result);
					eventloop.execute(() -> apply.whenComplete((u, throwable) -> {
						if (throwable != null) {
							completeExceptionally(throwable);
						} else {
							complete(u);
						}
						tracker.complete();
					}));
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	@Override
	public CompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
		return subscribe(new NextCompletionStage<T, T>() {
			@Override
			protected void onComplete(T value) {
				complete(value);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				complete(fn.apply(error));
			}
		});
	}

	@Override
	public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
		return subscribe(new NextCompletionStage<T, T>() {
			@Override
			protected void onComplete(T value) {
				action.accept(value, null);
				complete(value);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				action.accept(null, error);
				completeExceptionally(error);
			}
		});
	}

	@Override
	public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
		//noinspection unchecked
		return ((NextCompletionStage<T, T>) whenComplete(action)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		return subscribe(new NextCompletionStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					action.accept(result, null);
					eventloop.execute(() -> {
						complete(result);
						tracker.complete();
					});
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					action.accept(null, error);
					eventloop.execute(() -> {
						completeExceptionally(error);
						tracker.complete();
					});
				});
			}
		});
	}

	@Override
	public <U> CompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				U u = fn.apply(value, null);
				complete(u);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				U u = fn.apply(null, error);
				complete(u);
			}
		});
	}

	@Override
	public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
		//noinspection unchecked
		return ((NextCompletionStage<U, U>) handle(fn)).subscribe(new PostNextCompletionStage());
	}

	@Override
	public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
		final Eventloop eventloop = getCurrentEventloop();
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onComplete(T result) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					U u = fn.apply(result, null);
					eventloop.execute(() -> {
						complete(u);
						tracker.complete();
					});
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				final ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
				executor.execute(() -> {
					U u = fn.apply(null, error);
					eventloop.execute(() -> {
						complete(u);
						tracker.complete();
					});
				});
			}
		});
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		subscribe(new NextCompletionStage<T, Object>() {
			@Override
			protected void onComplete(T value) {
				future.complete(value);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				future.completeExceptionally(error);
			}
		});
		return future;
	}

	private static class PostNextCompletionStage<U> extends NextCompletionStage<U, U> {
		@Override
		protected void onComplete(U result) {
			getCurrentEventloop().post(() -> complete(result));
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			getCurrentEventloop().post(() -> completeExceptionally(error));
		}
	}

	private static class CompletedStage extends NextCompletionStage {
		@Override
		protected void onComplete(Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			throw new UnsupportedOperationException();
		}
	}
}
