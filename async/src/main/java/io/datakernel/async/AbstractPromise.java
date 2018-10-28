package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class AbstractPromise<T> implements Promise<T> {

	private static final BiConsumer<Object, Throwable> COMPLETED_PROMISE =
			(value, e) -> { throw new UnsupportedOperationException();};

	private static final BiConsumer<Object, Throwable> COMPLETED_EXCEPTIONALLY_PROMISE =
			(value, e) -> { throw new UnsupportedOperationException();};

	protected BiConsumer<? super T, Throwable> next;

	@Override
	public boolean isComplete() {
		return next == COMPLETED_PROMISE || next == COMPLETED_EXCEPTIONALLY_PROMISE;
	}

	@Override
	public boolean isResult() {
		return next == COMPLETED_PROMISE;
	}

	@Override
	public boolean isException() {
		return next == COMPLETED_EXCEPTIONALLY_PROMISE;
	}

	protected void complete(@Nullable T value, @Nullable Throwable error) {
		assert next != COMPLETED_PROMISE && next != COMPLETED_EXCEPTIONALLY_PROMISE;
		if (error == null) {
			complete(value);
		} else {
			completeExceptionally(error);
		}
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	protected void complete(@Nullable T value) {
		assert next != COMPLETED_PROMISE && next != COMPLETED_EXCEPTIONALLY_PROMISE;
		if (next != null) {
			next.accept(value, null);
			next = COMPLETED_PROMISE;
		}
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	protected void completeExceptionally(@Nullable Throwable error) {
		assert next != COMPLETED_PROMISE && next != COMPLETED_EXCEPTIONALLY_PROMISE;
		if (next != null) {
			next.accept(null, error);
			next = COMPLETED_EXCEPTIONALLY_PROMISE;
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
	public <U, S extends BiConsumer<? super T, Throwable> & Promise<U>> Promise<U> then(S promise) {
		subscribe(promise);
		return promise;
	}

	@SuppressWarnings("unchecked")
	protected void subscribe(BiConsumer<? super T, Throwable> consumer) {
		if (next == null) {
			next = consumer;
		} else {
			assert !isComplete() : "Promise has already been completed";
			final BiConsumer<? super T, Throwable> finalNext = this.next;
			next = (BiConsumer<T, Throwable>) (result, error) -> {
				finalNext.accept(result, error);
				consumer.accept(result, error);
			};
		}
	}

	@Override
	public <U> Promise<U> thenApply(Function<? super T, ? extends U> fn) {
		return then(new NextPromise<T, U>() {
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
	public <U> Promise<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		return then(new NextPromise<T, U>() {
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
	public <U> Promise<U> thenCompose(Function<? super T, ? extends Promise<U>> fn) {
		return then(new NextPromise<T, U>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					Promise<U> promise;
					try {
						promise = fn.apply(result);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					promise.whenComplete(this::complete);
				} else {
					completeExceptionally(e);
				}
			}
		});
	}

	@Override
	public <U> Promise<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Promise<U>> fn) {
		return then(new NextPromise<T, U>() {
			@Override
			public void accept(T result, Throwable e) {
				if (e == null) {
					Promise<U> promise;
					try {
						promise = fn.apply(result, null);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					promise.whenComplete(this::complete);
				} else {
					Promise<U> promise;
					try {
						promise = fn.apply(null, e);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					promise.whenComplete(this::complete);
				}
			}
		});
	}

	@Override
	public Promise<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		subscribe(action);
		return this;
	}

	@Override
	public Promise<T> whenResult(Consumer<? super T> action) {
		return whenComplete((result, e) -> {
			if (e == null) {
				action.accept(result);
			}
		});
	}

	@Override
	public Promise<T> whenException(Consumer<Throwable> action) {
		return whenComplete((result, e) -> {
			if (e != null) {
				action.accept(e);
			}
		});
	}

	@Override
	public Promise<T> thenException(Function<? super T, Throwable> fn) {
		return then(new NextPromise<T, T>() {
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
	private static class PromiseCombine<T, V, U> extends NextPromise<T, V> {
		final BiFunction<? super T, ? super U, ? extends V> fn;
		@Nullable
		T thisResult = (T) NO_RESULT;
		@Nullable
		U otherResult = (U) NO_RESULT;

		PromiseCombine(BiFunction<? super T, ? super U, ? extends V> fn) {
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
	public <U, V> Promise<V> combine(Promise<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		if (other instanceof CompletePromise) {
			return thenApply(result -> fn.apply(result, ((CompletePromise<U>) other).getResult()));
		}
		PromiseCombine<T, V, U> resultPromise = new PromiseCombine<>(fn);
		other.whenComplete((result, e) -> {
			if (e == null) {
				resultPromise.onOtherComplete(result);
			} else {
				resultPromise.onAnyException(e);
			}
		});
		return then(resultPromise);
	}

	@SuppressWarnings("unchecked")
	private static class PromiseBoth<T> extends NextPromise<T, Void> {
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
	public Promise<Void> both(Promise<?> other) {
		if (other instanceof CompletePromise) return toVoid();
		PromiseBoth<T> resultPromise = new PromiseBoth<>();
		other.whenComplete((result, e) -> {
			if (e == null) {
				if (--resultPromise.counter == 0) {
					resultPromise.complete(null);
				}
			} else {
				resultPromise.tryCompleteExceptionally(e);
			}
		});
		return then(resultPromise);
	}

	private static final class EitherPromise<T> extends NextPromise<T, T> {
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
	public Promise<T> either(Promise<? extends T> other) {
		if (other instanceof CompletePromise) {
			@SuppressWarnings("unchecked") CompletePromise<T> otherCompletePromise = (CompletePromise<T>) other;
			if (otherCompletePromise.isException()) return this;
			return otherCompletePromise;
		}
		EitherPromise<T> resultPromise = new EitherPromise<>();
		other.whenComplete((result, e) -> {
			if (e == null) {
				resultPromise.tryComplete(result);
			} else {
				if (++resultPromise.errors == 2) {
					resultPromise.completeExceptionally(e);
				}
			}
		});
		return then(resultPromise);
	}

	@Override
	public Promise<Try<T>> toTry() {
		return then(new NextPromise<T, Try<T>>() {
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
	public Promise<Void> toVoid() {
		return thenApply($ -> null);
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
