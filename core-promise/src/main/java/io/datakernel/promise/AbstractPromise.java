/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.promise;

import io.datakernel.async.callback.Callback;
import io.datakernel.common.collection.Try;
import io.datakernel.common.exception.UncheckedException;
import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

@SuppressWarnings({"unchecked", "WeakerAccess", "unused", "ConstantConditions"})
abstract class AbstractPromise<T> implements Promise<T> {
	private static final Object PROMISE_NOT_SET = new Object();

	protected T result = (T) PROMISE_NOT_SET;

	@Nullable
	protected Throwable exception;

	@Nullable
	protected Callback<? super T> next;

	@Override
	public final boolean isComplete() {
		return result != PROMISE_NOT_SET;
	}

	@Override
	public final boolean isResult() {
		return result != PROMISE_NOT_SET && exception == null;
	}

	@Override
	public final boolean isException() {
		return exception != null;
	}

	@Override
	public T getResult() {
		return result != PROMISE_NOT_SET ? result : null;
	}

	@Override
	public Throwable getException() {
		return exception;
	}

	@Override
	public Try<T> getTry() {
		if (isResult()) return Try.of(result);
		if (isException()) return Try.ofException(exception);
		return null;
	}

	protected void complete(@Nullable T value, @Nullable Throwable e) {
		assert !isComplete();
		if (e == null) {
			complete(value);
		} else {
			completeExceptionally(e);
		}
	}

	@Async.Execute
	protected void complete(@Nullable T value) {
		assert !isComplete();
		result = value;
		if (next != null) {
			next.accept(value, null);
		}
	}

	@Async.Execute
	protected void completeExceptionally(@Nullable Throwable e) {
		assert !isComplete();
		result = null;
		exception = e;
		if (next != null) {
			next.accept(null, e);
		}
	}

	protected void tryComplete(@Nullable T value, @Nullable Throwable e) {
		if (!isComplete()) {
			complete(value, e);
		}
	}

	protected void tryComplete(@Nullable T value) {
		if (!isComplete()) {
			complete(value);
		}
	}

	protected void tryCompleteExceptionally(@NotNull Throwable e) {
		if (!isComplete()) {
			completeExceptionally(e);
		}
	}

	@NotNull
	@Override
	public <U, P extends Callback<? super T> & Promise<U>> Promise<U> next(@Async.Schedule @NotNull P promise) {
		if (isComplete()) {
			promise.accept(result, exception);
			return promise;
		}
		subscribe(promise);
		return promise;
	}

	@Async.Schedule
	protected void subscribe(@NotNull Callback<? super T> consumer) {
		assert !isComplete();
		if (next == null) {
			next = consumer;
		} else {
			assert !isComplete() : "Promise has already been completed";
			Callback<? super T> finalNext = next;
			next = (Callback<T>) (result, e) -> {
				finalNext.accept(result, e);
				consumer.accept(result, e);
			};
		}
	}

	@NotNull
	@Override
	public <U> Promise<U> map(@NotNull Function<? super T, ? extends U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? Promise.of(fn.apply(result)) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<T, U>() {
			@Override
			public void accept(T result, @Nullable Throwable e) {
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
		};
		subscribe(resultPromise);
		return resultPromise;
	}

	@NotNull
	@Override
	public <U> Promise<U> mapEx(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
		if (isComplete()) {
			try {
				return Promise.of(fn.apply(result, exception));
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<T, U>() {
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
		};
		subscribe(resultPromise);
		return resultPromise;
	}

	@NotNull
	@Override
	public <U> Promise<U> then(@NotNull Function<? super T, ? extends Promise<? extends U>> fn) {
		if (isComplete()) {
			try {
				return isResult() ? (Promise<U>) fn.apply(result) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<T, U>() {
			@Override
			public void accept(T result, @Nullable Throwable e) {
				if (e == null) {
					Promise<? extends U> promise;
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
		};
		subscribe(resultPromise);
		return resultPromise;
	}

	@NotNull
	@Override
	public <U> Promise<U> thenEx(@NotNull BiFunction<? super T, Throwable, ? extends Promise<? extends U>> fn) {
		if (isComplete()) {
			try {
				return (Promise<U>) fn.apply(result, exception);
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		NextPromise<T, U> resultPromise = new NextPromise<T, U>() {
			@Override
			public void accept(T result, @Nullable Throwable e) {
				if (e == null) {
					Promise<? extends U> promise;
					try {
						promise = fn.apply(result, null);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					promise.whenComplete(this::complete);
				} else {
					Promise<? extends U> promise;
					try {
						promise = fn.apply(null, e);
					} catch (UncheckedException u) {
						completeExceptionally(u.getCause());
						return;
					}
					promise.whenComplete(this::complete);
				}
			}
		};
		subscribe(resultPromise);
		return resultPromise;
	}

	@NotNull
	@Override
	public Promise<T> whenComplete(@NotNull Callback<? super T> action) {
		if (isComplete()) {
			action.accept(result, exception);
			return this;
		}
		subscribe(action);
		return this;
	}

	@NotNull
	@Override
	public Promise<T> whenComplete(@NotNull Runnable action) {
		if (isComplete()) {
			action.run();
			return this;
		}
		subscribe((result, e) -> action.run());
		return this;
	}

	@NotNull
	@Override
	public Promise<T> whenResult(@NotNull Consumer<? super T> action) {
		if (isComplete()) {
			if (isResult()) action.accept(result);
			return this;
		}
		subscribe((result, e) -> {
			if (e == null) {
				action.accept(result);
			}
		});
		return this;
	}

	@Override
	public Promise<T> whenException(@NotNull Consumer<Throwable> action) {
		if (isComplete()) {
			if (isException()) {
				action.accept(exception);
			}
			return this;
		}
		subscribe((result, e) -> {
			if (e != null) {
				action.accept(e);
			}
		});
		return this;
	}

	private static final Object NO_RESULT = new Object();

	@NotNull
	@Override
	public Promise<T> async() {
		if (isComplete()) {
			SettablePromise<T> promise = new SettablePromise<>();
			getCurrentEventloop().post(exception == null ?
					() -> promise.set(result) :
					() -> promise.setException(exception));
			return promise;
		}
		return this;
	}

	@NotNull
	@Override
	public <U, V> Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
		if (isComplete()) {
			return isResult() ? other.map(otherResult -> fn.apply(getResult(), otherResult)) : (Promise<V>) this;
		}
		if (other.isComplete()) {
			return other.isResult() ? this.map(result -> fn.apply(result, other.getResult())) : (Promise<V>) other;
		}
		PromiseCombine<T, V, U> resultPromise = new PromiseCombine<>(fn);
		other.whenComplete(resultPromise::acceptOther);
		subscribe(resultPromise);
		return resultPromise;
	}

	@SuppressWarnings({"unchecked", "WeakerAccess"})
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
		public void accept(T result, @Nullable Throwable e) {
			if (e == null) {
				if (otherResult != NO_RESULT) {
					onBothResults(result, otherResult);
				} else {
					thisResult = result;
				}
			} else {
				onAnyException(e);
			}
		}

		public void acceptOther(U otherResult, @Nullable Throwable e) {
			if (e == null) {
				if (thisResult != NO_RESULT) {
					onBothResults(thisResult, otherResult);
				} else {
					this.otherResult = otherResult;
				}
			} else {
				tryCompleteExceptionally(e);
			}
		}

		void onBothResults(@Nullable T thisResult, @Nullable U otherResult) {
			tryComplete(fn.apply(thisResult, otherResult));
		}

		void onAnyException(@NotNull Throwable e) {
			tryCompleteExceptionally(e);
		}
	}

	@NotNull
	@Override
	public Promise<Void> both(@NotNull Promise<?> other) {
		if (isComplete()) {
			return isResult() ? other.toVoid() : (Promise<Void>) this;
		}
		if (other.isComplete()) {
			return other.isResult() ? toVoid() : (Promise<Void>) other;
		}
		PromiseBoth<Object> resultPromise = new PromiseBoth<>();
		other.whenComplete(resultPromise);
		subscribe(resultPromise);
		return resultPromise;
	}

	private static class PromiseBoth<T> extends NextPromise<T, Void> {
		int counter = 2;

		@Override
		public void accept(T result, @Nullable Throwable e) {
			if (e == null) {
				if (--counter == 0) {
					complete(null);
				}
			} else {
				tryCompleteExceptionally(e);
			}
		}
	}

	@NotNull
	@Override
	public Promise<T> either(@NotNull Promise<? extends T> other) {
		if (isComplete()) {
			return isResult() ? this : (Promise<T>) other;
		}
		if (other.isComplete()) {
			return other.isResult() ? (Promise<T>) other : this;
		}
		EitherPromise<T> resultPromise = new EitherPromise<>();
		other.whenComplete(resultPromise);
		subscribe(resultPromise);
		return resultPromise;
	}

	private static final class EitherPromise<T> extends NextPromise<T, T> {
		int errors;

		@Override
		public void accept(T result, @Nullable Throwable e) {
			if (e == null) {
				tryComplete(result);
			} else {
				if (++errors == 2) {
					completeExceptionally(e);
				}
			}
		}
	}

	@NotNull
	@Override
	public Promise<Try<T>> toTry() {
		if (isComplete()) {
			return Promise.of(isResult() ? Try.of(result) : Try.ofException(exception));
		}
		NextPromise<T, Try<T>> resultPromise = new NextPromise<T, Try<T>>() {
			@Override
			public void accept(T result, @Nullable Throwable e) {
				if (e == null) {
					complete(Try.of(result));
				} else {
					complete(Try.ofException(e));
				}
			}
		};
		subscribe(resultPromise);
		return resultPromise;
	}

	@NotNull
	@Override
	public Promise<Void> toVoid() {
		if (isComplete()) {
			return isResult() ? Promise.complete() : (Promise<Void>) this;
		}
		NextPromise<T, Void> resultPromise = new NextPromise<T, Void>() {
			@Override
			public void accept(T result, @Nullable Throwable e) {
				if (e == null) {
					complete(null);
				} else {
					completeExceptionally(e);
				}
			}
		};
		subscribe(resultPromise);
		return resultPromise;
	}

	@NotNull
	@Override
	public CompletableFuture<T> toCompletableFuture() {
		if (isComplete()) {
			if (isResult()) {
				return CompletableFuture.completedFuture(result);
			} else {
				CompletableFuture<T> future = new CompletableFuture<>();
				future.completeExceptionally(exception);
				return future;
			}
		}
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
