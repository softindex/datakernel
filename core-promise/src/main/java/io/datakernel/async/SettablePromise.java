/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.async;

import io.datakernel.exception.StacklessException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Represents a {@link Promise} which can be completed or completedExceptionally manually.
 * <p>
 * Can be used as root {@code Promise} to start execution of chain of
 * {@code Promises} or when you want wrap your actions in {@code Promise}.
 *
 * @param <T> result type
 */
public final class SettablePromise<T> extends AbstractPromise<T> implements MaterializedPromise<T>, SettableCallback<T> {
	private static final Throwable PROMISE_NOT_SET = new StacklessException(SettablePromise.class, "Promise has not been completed yet");

	private T result;

	@Nullable
	private Throwable exception = PROMISE_NOT_SET;

	@Override
	public boolean isComplete() {
		return exception != PROMISE_NOT_SET;
	}

	@Override
	public boolean isResult() {
		return exception == null;
	}

	@Override
	public boolean isException() {
		return exception != null && exception != PROMISE_NOT_SET;
	}

	/**
	 * Sets the result of this {@code SettablePromise} and
	 * completes it. {@code AssertionError} is thrown when you
	 * try to set result for an already completed {@code Promise}.
	 */
	@Override
	public void set(T result) {
		assert !isComplete();
		this.result = result;
		this.exception = null;
		complete(result);
	}

	/**
	 * Sets exception and completes this {@code SettablePromise} exceptionally.
	 * {@code AssertionError} is thrown when you try to set exception for
	 * an already completed {@code Promise}.
	 *
	 * @param e exception
	 */
	@Override
	public void setException(@NotNull Throwable e) {
		assert !isComplete();
		exception = e;
		completeExceptionally(e);
	}

	@Override
	protected void subscribe(@NotNull Callback<? super T> next) {
		assert !isComplete();
		super.subscribe(next);
	}

	@Nullable
	@Override
	public T getResult() {
		if (isResult()) {
			return result;
		}
		throw new IllegalStateException();
	}

	@NotNull
	@Override
	public Throwable getException() {
		if (isException()) {
			assert exception != null;
			return exception;
		}
		throw new IllegalStateException();
	}

	@Override
	public @NotNull Try<T> getTry() {
		if (isResult()) return Try.of(result);
		if (isException()) {
			//noinspection ConstantConditions
			return Try.ofException(exception);
		}
		throw new IllegalStateException();
	}

	@NotNull
	@Override
	public <U, P extends Callback<? super T> & Promise<U>> Promise<U> next(@NotNull P promise) {
		if (isComplete()) {
			promise.accept(result, exception);
			return promise;
		}
		return super.next(promise);
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> map(@NotNull Function<? super T, ? extends U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? Promise.of(fn.apply(result)) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.map(fn);
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
		return super.mapEx(fn);
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> then(@NotNull Function<? super T, ? extends Promise<U>> fn) {
		if (isComplete()) {
			try {
				return isResult() ? fn.apply(result) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.then(fn);
	}

	@NotNull
	@Override
	public <U> Promise<U> thenEx(@NotNull BiFunction<? super T, Throwable, ? extends Promise<U>> fn) {
		if (isComplete()) {
			try {
				return fn.apply(result, exception);
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenEx(fn);
	}

	@NotNull
	@Override
	public Promise<T> whenComplete(@NotNull Callback<? super T> action) {
		if (isComplete()) {
			action.accept(result, exception);
			return this;
		}
		return super.whenComplete(action);
	}

	@NotNull
	@Override
	public Promise<T> whenResult(@NotNull Consumer<? super T> action) {
		if (isComplete()) {
			if (isResult()) action.accept(result);
			return this;
		}
		return super.whenResult(action);
	}

	@Override
	public Promise<T> whenException(@NotNull Consumer<Throwable> action) {
		if (isComplete()) {
			if (isException()) {
				assert exception != null;
				action.accept(exception);
			}
			return this;
		}
		return super.whenException(action);
	}

	@NotNull
	@Override
	public MaterializedPromise<T> async() {
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
	public Promise<Try<T>> toTry() {
		if (isComplete()) {
			return Promise.of(Try.of(result, exception));
		}
		return super.toTry();
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public Promise<Void> toVoid() {
		if (isComplete()) {
			return isResult() ? Promise.complete() : (Promise<Void>) this;
		}
		return super.toVoid();
	}

	@NotNull
	@Override
	public <U, V> Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
		if (isComplete()) {
			return Promise.of(result, exception).combine(other, fn);
		}
		return super.combine(other, fn);
	}

	@NotNull
	@Override
	public Promise<Void> both(@NotNull Promise<?> other) {
		if (isComplete()) {
			return Promise.of(result, exception).both(other);
		}
		return super.both(other);
	}

	@NotNull
	@Override
	public Promise<T> either(@NotNull Promise<? extends T> other) {
		if (isComplete()) {
			return Promise.of(result, exception).either(other);
		}
		return super.either(other);
	}

	@NotNull
	@Override
	public CompletableFuture<T> toCompletableFuture() {
		if (isComplete()) {
			return Promise.of(result, exception).toCompletableFuture();
		}
		return super.toCompletableFuture();
	}

	@Override
	public String toString() {
		return "SettablePromise{" +
				(isComplete() ?
						(exception == null ?
								"" + result :
								"exception=" + exception.getClass().getSimpleName()) :
						"<unset>")
				+ "}";
	}
}
