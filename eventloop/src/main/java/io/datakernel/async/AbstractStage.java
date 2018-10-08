package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.Preconditions.checkArgument;

abstract class AbstractStage<T> implements Stage<T> {

	private static final BiConsumer<Object, Throwable> COMPLETED_STAGE =
			(value, e) -> { throw new UnsupportedOperationException();};

	private static final BiConsumer<Object, Throwable> COMPLETED_EXCEPTIONALLY_STAGE =
			(value, e) -> { throw new UnsupportedOperationException();};

	protected BiConsumer<? super T, Throwable> next;

	@Override
	public boolean isComplete() {
		return next == COMPLETED_STAGE || next == COMPLETED_EXCEPTIONALLY_STAGE;
	}

	@Override
	public boolean isResult() {
		return next == COMPLETED_STAGE;
	}

	@Override
	public boolean isException() {
		return next == COMPLETED_EXCEPTIONALLY_STAGE;
	}

	protected void complete(@Nullable T value, @Nullable Throwable error) {
		assert next != COMPLETED_STAGE && next != COMPLETED_EXCEPTIONALLY_STAGE;
		if (error == null) {
			complete(value);
		} else {
			completeExceptionally(error);
		}
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	protected void complete(@Nullable T value) {
		assert next != COMPLETED_STAGE && next != COMPLETED_EXCEPTIONALLY_STAGE;
		if (next != null) {
			next.accept(value, null);
			next = COMPLETED_STAGE;
		}
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	protected void completeExceptionally(@Nullable Throwable error) {
		assert next != COMPLETED_STAGE && next != COMPLETED_EXCEPTIONALLY_STAGE;
		if (next != null) {
			next.accept(null, error);
			next = COMPLETED_EXCEPTIONALLY_STAGE;
		}
	}

	protected void tryComplete(@Nullable T value, @Nullable Throwable error) {
		if (!isComplete()) {
			complete(value, error);
		}
	}

	protected void tryComplete(@Nullable T value) {
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
	public <U, S extends BiConsumer<? super T, Throwable> & Stage<U>> Stage<U> then(S stage) {
		subscribe(stage);
		return stage;
	}

	@SuppressWarnings("unchecked")
	protected void subscribe(BiConsumer<? super T, Throwable> consumer) {
		if (next == null) {
			next = consumer;
		} else {
			assert !isComplete() : "Stage has already been completed";
			final BiConsumer<? super T, Throwable> finalNext = this.next;
			next = (BiConsumer<T, Throwable>) (result, error) -> {
				finalNext.accept(result, error);
				consumer.accept(result, error);
			};
		}
	}

	@Override
	public <U> Stage<U> thenApply(Function<? super T, ? extends U> fn) {
		return then(new NextStage<T, U>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					U newResult;
					try {
						newResult = fn.apply(result);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					complete(newResult);
				} else {
					completeExceptionally(e);
				}
			}
		});
	}

	@Override
	public <U> Stage<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		return then(new NextStage<T, U>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					U newResult;
					try {
						newResult = fn.apply(result, null);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					complete(newResult);
				} else {
					U newResult;
					try {
						newResult = fn.apply(null, e);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					complete(newResult);
				}
			}
		});
	}

	@Override
	public <U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn) {
		return then(new NextStage<T, U>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					Stage<U> stage;
					try {
						stage = fn.apply(result);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					stage.whenComplete(this::complete);
				} else {
					completeExceptionally(e);
				}
			}
		});
	}

	@Override
	public <U> Stage<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Stage<U>> fn) {
		return then(new NextStage<T, U>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					Stage<U> stage;
					try {
						stage = fn.apply(result, null);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					stage.whenComplete(this::complete);
				} else {
					Stage<U> stage;
					try {
						stage = fn.apply(null, e);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					stage.whenComplete(this::complete);
				}
			}
		});
	}

	@Override
	public Stage<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		subscribe(action);
		return this;
	}

	@Override
	public Stage<T> whenResult(Consumer<? super T> action) {
		return whenComplete((result, e) -> {
			if (e == null) {
				action.accept(result);
			}
		});
	}

	@Override
	public Stage<T> whenException(Consumer<Throwable> action) {
		return whenComplete((result, e) -> {
			if (e != null) {
				action.accept(e);
			}
		});
	}

	@Override
	public Stage<T> thenException(Function<? super T, Throwable> fn) {
		return then(new NextStage<T, T>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					Throwable maybeException = fn.apply(result);
					if (maybeException == null) {
						complete(result);
					} else {
						completeExceptionally(maybeException);
					}
				} else {
					completeExceptionally(e);
				}
			}
		});
	}

	private static final Object NO_RESULT = new Object();

	@SuppressWarnings("unchecked")
	private static class StageCombine<T, V, U> extends NextStage<T, V> {
		final BiFunction<? super T, ? super U, ? extends V> fn;
		@Nullable
		T thisResult = (T) NO_RESULT;
		@Nullable
		U otherResult = (U) NO_RESULT;

		StageCombine(BiFunction<? super T, ? super U, ? extends V> fn) {
			this.fn = fn;
		}

		@Override
		public void accept(T result, Throwable e) {
			if (e == null) {
				if (otherResult != NO_RESULT) {
					onBothResults(result, otherResult);
				} else {
					this.thisResult = result;
				}
			} else {
				onAnyException(e);
			}
		}

		protected void onOtherComplete(@Nullable U otherResult) {
			if (thisResult != NO_RESULT) {
				onBothResults(thisResult, otherResult);
			} else {
				this.otherResult = otherResult;
			}
		}

		void onBothResults(@Nullable T thisResult, @Nullable U otherResult) {
			this.thisResult = null;
			this.otherResult = null;
			if (!isComplete()) {
				complete(fn.apply(thisResult, otherResult));
			}
		}

		void onAnyException(Throwable e) {
			tryCompleteExceptionally(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U, V> Stage<V> combine(Stage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		if (other instanceof CompleteStage) {
			return thenApply(result -> fn.apply(result, ((CompleteStage<U>) other).getResult()));
		}
		StageCombine<T, V, U> resultStage = new StageCombine<>(fn);
		other.whenComplete((result, e) -> {
			if (e == null) {
				resultStage.onOtherComplete(result);
			} else {
				resultStage.onAnyException(e);
			}
		});
		return then(resultStage);
	}

	@SuppressWarnings("unchecked")
	private static class StageBoth<T> extends NextStage<T, Void> {
		int counter = 2;

		@Override
		public void accept(T result, Throwable e) {
			if (e == null) {
				if (--counter == 0) {
					complete(null);
				}
			} else {
				tryCompleteExceptionally(e);
			}
		}
	}

	@Override
	public Stage<Void> both(Stage<?> other) {
		if (other instanceof CompleteStage) return toVoid();
		StageBoth<T> resultStage = new StageBoth<>();
		other.whenComplete((result, e) -> {
			if (e == null) {
				if (--resultStage.counter == 0) {
					resultStage.complete(null);
				}
			} else {
				resultStage.tryCompleteExceptionally(e);
			}
		});
		return then(resultStage);
	}

	private static final class EitherStage<T> extends NextStage<T, T> {
		int errors;

		@Override
		public void accept(T result, Throwable e) {
			if (e == null) {
				tryComplete(result);
			} else {
				if (++errors == 2) {
					completeExceptionally(e);
				}
			}
		}
	}

	@Override
	public Stage<T> either(Stage<? extends T> other) {
		if (other instanceof CompleteStage) {
			@SuppressWarnings("unchecked") CompleteStage<T> otherCompleteStage = (CompleteStage<T>) other;
			if (otherCompleteStage.isException()) return this;
			return otherCompleteStage;
		}
		EitherStage<T> resultStage = new EitherStage<>();
		other.whenComplete((result, e) -> {
			if (e == null) {
				resultStage.tryComplete(result);
			} else {
				if (++resultStage.errors == 2) {
					resultStage.completeExceptionally(e);
				}
			}
		});
		return then(resultStage);
	}

	@Override
	public Stage<Try<T>> toTry() {
		return then(new NextStage<T, Try<T>>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					complete(Try.of(result));
				} else {
					complete(Try.ofException(e));
				}
			}
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stage<Void> toVoid() {
		return thenApply($ -> null);
	}

	@Override
	public Stage<T> timeout(@Nullable Duration timeout) {
		if (timeout == null) {
			return this;
		}
		checkArgument(timeout.toMillis() >= 0, "Timeout cannot be less than zero");
		ScheduledRunnable schedule = getCurrentEventloop().delay(timeout, () -> tryCompleteExceptionally(TIMEOUT_EXCEPTION));
		return then(new NextStage<T, T>() {
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

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		subscribe((result, e) -> {
			if (e == null) {
				future.complete(result);
			} else {
				future.completeExceptionally(e);
			}
		});
		return future;
	}
}
