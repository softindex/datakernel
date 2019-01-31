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
import java.util.function.BiConsumer;
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
public final class SettablePromise<T> extends AbstractPromise<T> implements MaterializedPromise<T> {
	private static final Throwable PROMISE_NOT_SET = new StacklessException(SettablePromise.class, "Promise has not been completed yet");

	@Nullable
	private T result;

	@Nullable
	private Throwable exception = PROMISE_NOT_SET;

	/**
	 * Returns a {@code SettablePromise} created from
	 * {@code Promise} after it completes.
	 */
	@NotNull
	public static <T> SettablePromise<T> ofPromise(@NotNull Promise<T> promise) {
		SettablePromise<T> result = new SettablePromise<>();
		promise.whenComplete(result::set);
		return result;
	}

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
	public void set(@Nullable T result) {
		assert !isComplete();
		this.result = result;
		this.exception = null;
		complete(result);
	}

	public void set(@Nullable T result, @Nullable Throwable e) {
		if (e == null) {
			set(result);
		} else {
			setException(e);
		}
	}

	/**
	 * Sets exception and completes this {@code SettablePromise} exceptionally.
	 * {@code AssertionError} is thrown when you try to set exception for
	 * an already completed {@code Promise}.
	 *
	 * @param e exception
	 */
	public void setException(@NotNull Throwable e) {
		assert !isComplete();
		result = null;
		exception = e;
		completeExceptionally(e);
	}

	/**
	 * Tries to set provided {@code result} for this
	 * {@code SettablePromise} if it is not completed yet.
	 */
	public void trySet(@Nullable T result) {
		if (isComplete()) {
			return;
		}
		set(result);
	}

	/**
	 * Tries to set result or exception for this {@code SettablePromise}
	 * if it not completed yet. Otherwise does nothing.
	 */
	public void trySet(@Nullable T result, @Nullable Throwable e) {
		if (isComplete()) {
			return;
		}
		if (e == null) {
			trySet(result);
		} else {
			trySetException(e);
		}
	}

	/**
	 * Tries to set provided {@code e} exception for this
	 * {@code SettablePromise} if it is not completed yet.
	 */
	public void trySetException(@NotNull Throwable e) {
		if (isComplete()) {
			return;
		}
		setException(e);
	}

	public void post(@Nullable T result) {
		getCurrentEventloop().post(() -> set(result));
	}

	public void postException(@NotNull Throwable e) {
		getCurrentEventloop().post(() -> setException(e));
	}

	public void post(@Nullable T result, @Nullable Throwable e) {
		getCurrentEventloop().post(() -> set(result, e));
	}

	public void tryPost(@Nullable T result) {
		getCurrentEventloop().post(() -> trySet(result));
	}

	public void tryPostException(@NotNull Throwable e) {
		getCurrentEventloop().post(() -> trySetException(e));
	}

	public void tryPost(@Nullable T result, @Nullable Throwable e) {
		getCurrentEventloop().post(() -> trySet(result, e));
	}

	@Override
	protected void subscribe(@NotNull BiConsumer<? super T, Throwable> next) {
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
	public <U, S extends BiConsumer<? super T, Throwable> & Promise<U>> Promise<U> then(@NotNull S promise) {
		if (isComplete()) {
			promise.accept(result, exception);
			return promise;
		}
		return super.then(promise);
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> thenApply(@NotNull Function<? super T, ? extends U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? Promise.of(fn.apply(result)) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenApply(fn);
	}

	@NotNull
	@Override
	public <U> Promise<U> thenApplyEx(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
		if (isComplete()) {
			try {
				return Promise.of(fn.apply(result, exception));
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenApplyEx(fn);
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> thenCompose(@NotNull Function<? super T, ? extends Promise<U>> fn) {
		if (isComplete()) {
			try {
				return isResult() ? fn.apply(result) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenCompose(fn);
	}

	@NotNull
	@Override
	public <U> Promise<U> thenComposeEx(@NotNull BiFunction<? super T, Throwable, ? extends Promise<U>> fn) {
		if (isComplete()) {
			try {
				return fn.apply(result, exception);
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenComposeEx(fn);
	}

	@NotNull
	@Override
	public Promise<T> whenComplete(@NotNull BiConsumer<? super T, Throwable> action) {
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

	@NotNull
	@Override
	public Promise<T> whenException(@NotNull Consumer<Throwable> action) {
		if (isComplete()) {
			if (isException()) action.accept(exception);
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
