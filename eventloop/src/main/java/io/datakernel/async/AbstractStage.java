package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage.Handler.Completion;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.functional.Try;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.Preconditions.checkArgument;

abstract class AbstractStage<T> implements Stage<T> {

	private static final StageConsumer<Object> COMPLETED_STAGE = (t, throwable) -> {
		throw new UnsupportedOperationException();
	};

	protected StageConsumer<? super T> next;

	public boolean isComplete() {
		return next == COMPLETED_STAGE;
	}

	protected void complete(@Nullable T value, @Nullable Throwable error) {
		assert !isComplete();
		if (error == null) {
			complete(value);
		} else {
			completeExceptionally(error);
		}
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	protected void complete(@Nullable T value) {
		assert !isComplete();
		if (next != null) {
			next.accept(value, null);
			next = COMPLETED_STAGE;
		}
		assert (next = COMPLETED_STAGE) != null;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	protected void completeExceptionally(@Nullable Throwable error) {
		assert !isComplete();
		if (next != null) {
			next.accept(null, error);
			next = COMPLETED_STAGE;
		}
		assert (next = COMPLETED_STAGE) != null;
	}

	protected void tryComplete(@Nullable T value, @Nullable Throwable error) {
		if (!isComplete()) {
			complete(value, error);
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

	@Override
	public <U, S extends StageConsumer<? super T> & Stage<U>> Stage<U> then(S stage) {
		subscribe(stage);
		return stage;
	}

	@SuppressWarnings("unchecked")
	protected void subscribe(StageConsumer<? super T> consumer) {
		if (next == null) {
			next = consumer;
		} else {
			assert next != COMPLETED_STAGE : "Stage has already been completed";
			if (consumer instanceof NextStage) {
				NextStage nextStage = (NextStage) consumer;
				assert nextStage.next == null;
				nextStage.prev = next;
				next = consumer;
			} else {
				final StageConsumer<? super T> finalNext = this.next;
				next = (StageConsumer<T>) (result, error) -> {
					finalNext.accept(result, error);
					consumer.accept(result, error);
				};
			}
		}
	}

	private static abstract class HandlerStage<F, T> extends NextStage<F, T> implements Completion<T> {
		@Override
		public void complete(T result) {
			super.complete(result);
		}

		@Override
		public void completeExceptionally(Throwable t) {
			super.completeExceptionally(t);
		}

		@Override
		public void complete(T result, Throwable throwable) {
			super.complete(result, throwable);
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

			private void doComplete(@Nullable T value, @Nullable Throwable error) {
				Eventloop eventloop = getCurrentEventloop();
				eventloop.startExternalTask();
				executor.execute(() -> handler.handle(value, error, new Completion<U>() {
					@Override
					public void complete(U result) {
						eventloop.execute(() -> complete(result));
						eventloop.completeExternalTask();
					}

					@Override
					public void completeExceptionally(Throwable t) {
						eventloop.execute(() -> completeExceptionally(t));
						eventloop.completeExternalTask();
					}
				}));
			}
		});
	}

	@Override
	public <U> Stage<U> thenApply(Function<? super T, ? extends U> fn) {
		return then(new NextStage<T, U>() {
			@Override
			protected void onComplete(T result) {
				U newResult = fn.apply(result);
				complete(newResult);
			}
		});
	}

	@Override
	public Stage<T> thenAccept(Consumer<? super T> action) {
		return whenComplete((result, throwable) -> {
			if (throwable == null) {
				action.accept(result);
			}
		});
	}

	@Override
	public Stage<T> thenRun(Runnable action) {
		return whenComplete((result, throwable) -> {
			if (throwable == null) {
				action.run();
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
		});
	}

	@Override
	public Stage<T> whenComplete(StageConsumer<? super T> action) {
		subscribe(action);
		return this;
	}

	@Override
	public Stage<T> whenException(Consumer<? super Throwable> action) {
		return whenComplete((result, throwable) -> {
			if (throwable != null) {
				action.accept(throwable);
			}
		});
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

	@Override
	public Stage<T> mapFailure(Function<Throwable, Throwable> fn) {
		return then(new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				complete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(fn.apply(throwable));
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
	public Stage<T> post() {
		return then(new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				getCurrentEventloop().post(() -> complete(result));
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				getCurrentEventloop().post(() -> completeExceptionally(throwable));
			}
		});
	}

	@Override
	public Stage<Try<T>> toTry() {
		return then(new NextStage<T, Try<T>>() {
			@Override
			protected void onComplete(T result) {
				complete(Try.of(result));
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				complete(Try.ofFailure(throwable));
			}
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<Void> toVoid() {
		return thenApply($ -> null);
	}

	@Override
	public Stage<T> timeout(long timeout) {
		checkArgument(timeout >= 0, "Timeout cannot be less than zero");
		if (timeout == 0) {
			return this;
		}
		ScheduledRunnable schedule = getCurrentEventloop().delay(timeout, () -> tryCompleteExceptionally(TIMEOUT_EXCEPTION));
		return then(new NextStage<T, T>() {

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
