package io.datakernel.async;

import io.datakernel.async.Stage.Handler.StageCallback;
import io.datakernel.eventloop.Eventloop;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

abstract class AbstractStage<T> implements Stage<T> {

	private static final BiConsumer<Object, Throwable> COMPLETED_STAGE = (t, throwable) -> {
		throw new UnsupportedOperationException();
	};

	protected BiConsumer<? super T, ? super Throwable> next;

	public boolean isComplete() {
		return next == COMPLETED_STAGE;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions", "unchecked"})
	protected void complete(T value) {
		assert !isComplete();
		if (next != null) {
			next.accept(value, null);
			next = COMPLETED_STAGE;
		}
		assert (next = COMPLETED_STAGE) != null;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions", "unchecked", "WeakerAccess"})
	protected void completeExceptionally(Throwable error) {
		assert !isComplete();
		if (next != null) {
			next.accept(null, error);
			next = COMPLETED_STAGE;
		}
		assert (next = COMPLETED_STAGE) != null;
	}

	protected void complete(T value, Throwable error) {
		assert !isComplete();
		if (error == null) {
			complete(value);
		} else {
			completeExceptionally(error);
		}
	}

	protected void tryComplete(T value) {
		if (!isComplete()) {
			complete(value);
		}
	}

	protected void tryCompleteExceptionally(Throwable error) {
		if (!isComplete()) {
			completeExceptionally(error);
		}
	}

	protected void tryComplete(T value, Throwable error) {
		if (!isComplete()) {
			complete(value, error);
		}
	}

	static final class StageForwarder<T> extends NextStage<T, T> {
		final BiConsumer<? super T, ? super Throwable> prev;

		StageForwarder(BiConsumer<? super T, ? super Throwable> prev,
		               BiConsumer<? super T, ? super Throwable> next) {
			this.prev = prev;
			this.next = next;
		}

		@Override
		protected void onComplete(T result) {
			prev.accept(result, null);
			complete(result);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			prev.accept(null, throwable);
			completeExceptionally(throwable);
		}
	}

	@SuppressWarnings("unchecked")
	protected void subscribe(BiConsumer<? super T, ? super Throwable> consumer) {
		if (this.next == null) {
			this.next = consumer;
		} else {
			if (this.next == COMPLETED_STAGE)
				throw new IllegalStateException("Stage has already been completed");
			this.next = new StageForwarder<>(this.next, consumer);
		}
	}

	@Override
	public <U, S extends BiConsumer<? super T, ? super Throwable> & Stage<U>> Stage<U> then(S stage) {
		subscribe(stage);
		return stage;
	}

	private static abstract class HandlerStage<F, T> extends NextStage<F, T> implements StageCallback<T> {
		@Override
		public void complete(T result) {
			super.complete(result);
		}

		@Override
		public void completeExceptionally(Throwable t) {
			super.completeExceptionally(t);
		}
	}

	@Override
	public <U> Stage<U> handle(Handler<? super T, U> handler) {
		return then(new HandlerStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				handler.handle(value, null, this);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				handler.handle(null, error, this);
			}

		});
	}

	@Override
	public <U> Stage<U> handleAsync(Handler<? super T, U> handler) {
		return then(new HandlerStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				getCurrentEventloop().post(() -> handler.handle(value, null, this));
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				getCurrentEventloop().post(() -> handler.handle(null, error, this));
			}
		});
	}

	@Override
	public <U> Stage<U> handleAsync(Handler<? super T, U> handler, Executor executor) {
		return then(new HandlerStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				doComplete(value, null);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				doComplete(null, error);
			}

			private void doComplete(T value, Throwable error) {
				Eventloop eventloop = getCurrentEventloop();
				executor.execute(() -> handler.handle(value, error, new StageCallback<U>() {
					@Override
					public void complete(U result) {
						eventloop.execute(() -> complete(result));
					}

					@Override
					public void completeExceptionally(Throwable t) {
						eventloop.execute(() -> completeExceptionally(t));
					}
				}));
			}
		});
	}

	@Override
	public <U> Stage<U> thenApply(Function<? super T, ? extends U> fn) {
		return then(new NextStage<T, U>() {
			@Override
			protected void onComplete(T result1) {
				U newResult = fn.apply(result1);
				complete(newResult);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		});
	}

	@Override
	public Stage<Void> thenAccept(Consumer<? super T> action) {
		return then(new NextStage<T, Void>() {
			@Override
			protected void onComplete(T result1) {
				action.accept(result1);
				complete(null);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		});
	}

	@Override
	public Stage<Void> thenRun(Runnable action) {
		return then(new NextStage<T, Void>() {
			@Override
			protected void onComplete(T result1) {
				action.run();
				complete(null);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}

		});
	}

	@Override
	public <U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn) {
		return then(new NextStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				Stage<U> stage = fn.apply(value);
				if (stage instanceof SettableStage) {
					SettableStage<U> settableStage = (SettableStage<U>) stage;
					if (settableStage.isSet()) {
						if (settableStage.exception == null) {
							complete(settableStage.result);
						} else {
							completeExceptionally(settableStage.exception);
						}
						return;
					}
				}
				stage.whenComplete(this::complete);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				this.completeExceptionally(error);
			}
		});
	}

	@Override
	public Stage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
		subscribe(action);
		return this;
	}

	@Override
	public Stage<T> exceptionally(Function<? super Throwable, ? extends T> fn) {
		return then(new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				complete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				complete(fn.apply(throwable));
			}
		});
	}

	private static final Object NO_RESULT = new Object();

	@SuppressWarnings("unchecked")
	private static class StageCombine<T, V, U> extends NextStage<T, V> {
		final BiFunction<? super T, ? super U, ? extends V> fn;
		T thisResult = (T) NO_RESULT;
		U otherResult = (U) NO_RESULT;

		StageCombine(BiFunction<? super T, ? super U, ? extends V> fn) {
			this.fn = fn;
		}

		@Override
		protected void onComplete(T thisResult) {
			if (otherResult != NO_RESULT) {
				onBothResults(thisResult, otherResult);
			} else {
				this.thisResult = thisResult;
			}
		}

		protected void onOtherComplete(U otherResult) {
			if (thisResult != NO_RESULT) {
				onBothResults(thisResult, otherResult);
			} else {
				this.otherResult = otherResult;
			}
		}

		void onBothResults(T thisResult, U otherResult) {
			this.thisResult = null;
			this.otherResult = null;
			if (!isComplete()) {
				complete(fn.apply(thisResult, otherResult));
			}
		}

		void onAnyException(Throwable throwable) {
			tryCompleteExceptionally(throwable);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			onAnyException(throwable);
		}
	}

	@Override
	public <U, V> Stage<V> combine(Stage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		StageCombine<T, V, U> resultStage = new StageCombine<>(fn);
		other.whenComplete((result, throwable) -> {
			if (throwable == null) {
				resultStage.onOtherComplete(result);
			} else {
				resultStage.onAnyException(throwable);
			}
		});
		return then(resultStage);
	}

	@SuppressWarnings("unchecked")
	private static class StageBoth<T> extends NextStage<T, Void> {
		int counter = 2;

		@Override
		protected void onComplete(T thisResult) {
			if (--counter == 0) {
				complete(null);
			}
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			tryCompleteExceptionally(throwable);
		}
	}

	@Override
	public Stage<Void> both(Stage<?> other) {
		StageBoth<T> resultStage = new StageBoth<>();
		other.whenComplete((result, throwable) -> {
			if (throwable == null) {
				if (--resultStage.counter == 0) {
					resultStage.complete(null);
				}
			} else {
				resultStage.tryCompleteExceptionally(throwable);
			}
		});
		return then(resultStage);
	}

	private static final class EitherStage<T> extends NextStage<T, T> {
		int errors;

		@Override
		protected void onComplete(T result) {
			tryComplete(result);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			if (++errors == 2) {
				completeExceptionally(throwable);
			}
		}
	}

	@Override
	public Stage<T> either(Stage<? extends T> other) {
		EitherStage<T> resultStage = new EitherStage<>();
		other.whenComplete((result, throwable) -> {
			if (throwable == null) {
				resultStage.tryComplete(result);
			} else {
				if (++resultStage.errors == 2) {
					resultStage.completeExceptionally(throwable);
				}
			}
		});
		return then(resultStage);
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		subscribe((result, throwable) -> {
			if (throwable == null) {
				future.complete(result);
			} else {
				future.completeExceptionally(throwable);
			}
		});
		return future;
	}

}
