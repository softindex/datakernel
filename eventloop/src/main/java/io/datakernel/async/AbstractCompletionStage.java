package io.datakernel.async;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractCompletionStage<T> implements CompletionStage<T> {
	protected static abstract class NextCompletionStage<F, T> extends AbstractCompletionStage<T> {

		protected abstract void onResult(F result);

		protected void onError(Throwable error) {
			completeExceptionally(error);
		}
	}

 	private static final NextCompletionStage COMPLETED_STAGE = new NextCompletionStage() {
		@Override
		protected void onResult(Object result) {
			throw new IllegalStateException();
		}

		@Override
		protected void onError(Throwable error) {
			throw new IllegalStateException();
		}
	};

	protected NextCompletionStage<T, ?> next;

	protected boolean isComplete() {
		return next == COMPLETED_STAGE;
	}

	protected final void complete(T result) {
		assert !isComplete();
		if (next != null) {
			next.onResult(result);
			next = COMPLETED_STAGE;
		}
	}

	protected void completeExceptionally(Throwable error) {
		assert !isComplete();
		if (next != null) {
			next.onError(error);
			next = COMPLETED_STAGE;
		}
	}

	private static final class SplittingStage<T> extends NextCompletionStage<T, T> {
		private final ArrayList<NextCompletionStage<T, ?>> list = new ArrayList<>();

		private static <T> SplittingStage<T> convertOf(NextCompletionStage<T, ?> existing) {
			SplittingStage<T> stage = new SplittingStage<>();
			stage.list.add(existing);
			return stage;
		}

		@Override
		protected void onResult(T result) {
			for (NextCompletionStage<T, ?> next : list) {
				next.onResult(result);
			}
		}

		@Override
		protected void onError(Throwable error) {
			for (NextCompletionStage<T, ?> next : list) {
				next.onError(error);
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
				this.next = SplittingStage.convertOf(this.next);
			}
		}
		return next;
	}

	@Override
	public <U> CompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onResult(T input) {
				complete(fn.apply(input));
			}
		});
	}

	@Override
	public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
		return null;
	}

	@Override
	public <U> CompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
		return null;
	}

	@Override
	public CompletionStage<Void> thenAccept(Consumer<? super T> action) {
		return subscribe(new NextCompletionStage<T, Void>() {
			@Override
			protected void onResult(T result) {
				action.accept(result);
				complete(null);
			}
		});
	}

	@Override
	public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action) {
		return null;
	}

	@Override
	public CompletionStage<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
		return null;
	}

	@Override
	public CompletionStage<Void> thenRun(Runnable action) {
		return subscribe(new NextCompletionStage<T, Void>() {
			@Override
			protected void onResult(T result) {
				action.run();
				complete(null);
			}
		});
	}

	@Override
	public CompletionStage<Void> thenRunAsync(Runnable action) {
		return null;
	}

	@Override
	public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
		return null;
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
		protected void onResult(T result) {
			thisResult = result;
			tryComplete();
		}

		@Override
		protected void onError(Throwable error) {
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
				resultingStage.onError(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		return null;
	}

	@Override
	public <U, V> CompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
		return null;
	}

	private static final class ThenAcceptBothStage<T, U> extends NextCompletionStage<T, Void> {
		private final BiConsumer<? super T, ? super U> consumer;
		private int countdown = 2;
		private T thisResult;
		private U otherResult;

		private ThenAcceptBothStage(BiConsumer<? super T, ? super U> consumer) {
			this.consumer = consumer;
		}

		private void tryComplete() {
			if (--countdown == 0) {
				consumer.accept(thisResult, otherResult);
				thisResult = null;
				otherResult = null;
				complete(null);
			}
		}

		@Override
		protected void onResult(T result) {
			thisResult = result;
			tryComplete();
		}

		@Override
		protected void onError(Throwable error) {
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
				resultingStage.otherResult = result;
				resultingStage.tryComplete();
			} else {
				resultingStage.onError(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
		return null;
	}

	@Override
	public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
		return null;
	}

	private static final class RunAfterBothStage<T> extends NextCompletionStage<T, Void> {
		private final Runnable action;
		private int countdown = 2;

		private RunAfterBothStage(Runnable action) {
			this.action = action;
		}

		@Override
		protected void onResult(T result) {
			if (--countdown == 0) {
				action.run();
				complete(null);
			}
		}

		@Override
		protected void onError(Throwable error) {
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
				resultingStage.onResult(null);
			} else {
				resultingStage.onError(throwable);
			}
		});

		return subscribe(resultingStage);
	}

	@Override
	public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
		return null;
	}

	@Override
	public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
		return null;
	}

	@Override
	public <U> CompletionStage<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
		NextCompletionStage<T, U> resultingStage = new NextCompletionStage<T, U>() {
			@Override
			protected void onResult(T result) {
				if (!isComplete()) {
					complete(fn.apply(result));
				}
			}

			@Override
			protected void onError(Throwable error) {
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
		return null;
	}

	@Override
	public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
		return null;
	}

	@Override
	public CompletionStage<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
		NextCompletionStage<T, Void> resultingStage = new NextCompletionStage<T, Void>() {
			@Override
			protected void onResult(T result) {
				if (!isComplete()) {
					action.accept(result);
					complete(null);
				}
			}

			@Override
			protected void onError(Throwable error) {
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
		return null;
	}

	@Override
	public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
		return null;
	}

	@Override
	public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
		NextCompletionStage<T, Void> resultingStage = new NextCompletionStage<T, Void>() {
			@Override
			protected void onResult(T result) {
				if (!isComplete()) {
					action.run();
					complete(null);
				}
			}

			@Override
			protected void onError(Throwable error) {
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
		return null;
	}

	@Override
	public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
		return null;
	}

	@Override
	public <U> CompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onResult(T result) {
				fn.apply(result)
						.whenComplete((u, throwable) -> {
							if (throwable != null) {
								completeExceptionally(throwable);
							} else {
								complete(u);
							}
						});
			}

			@Override
			protected void onError(Throwable error) {
				completeExceptionally(error);
			}
		});
	}

	@Override
	public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
		return null;
	}

	@Override
	public <U> CompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
		return null;
	}

	@Override
	public CompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
		return subscribe(new NextCompletionStage<T, T>() {
			@Override
			protected void onResult(T result) {
				complete(result);
			}

			@Override
			protected void onError(Throwable error) {
				complete(fn.apply(error));
			}
		});
	}

	@Override
	public CompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
		return subscribe(new NextCompletionStage<T, T>() {
			@Override
			protected void onResult(T result) {
				action.accept(result, null);
				complete(result);
			}

			@Override
			protected void onError(Throwable error) {
				action.accept(null, error);
				completeExceptionally(error);
			}
		});
	}

	@Override
	public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
		return null;
	}

	@Override
	public CompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
		return null;
	}

	@Override
	public <U> CompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
		return subscribe(new NextCompletionStage<T, U>() {
			@Override
			protected void onResult(T result) {
				U u = fn.apply(result, null);
				complete(u);
			}

			@Override
			protected void onError(Throwable error) {
				U u = fn.apply(null, error);
				complete(u);
			}
		});
	}

	@Override
	public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
		return null;
	}

	@Override
	public <U> CompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
		return null;
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		subscribe(new NextCompletionStage<T, Object>() {
			@Override
			protected void onResult(T result) {
				future.complete(result);
			}

			@Override
			protected void onError(Throwable error) {
				future.completeExceptionally(error);
			}
		});
		return future;
	}

}
