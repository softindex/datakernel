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

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.StacklessException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Promise that can be completed or completedExceptionally manually.
 * <p>Can be used as root promise to start execution of chain of promises or when you want wrap your actions in {@code Promise}</p>
 *
 * @param <T> Result type
 */
public final class SettablePromise<T> extends AbstractPromise<T> implements MaterializedPromise<T> {
	private static final Throwable PROMISE_NOT_SET = new StacklessException(SettablePromise.class, "Promise has not been completed yet");

	@SuppressWarnings("unchecked")
	@Nullable
	protected T result;

	@Nullable
	protected Throwable exception = PROMISE_NOT_SET;

	public SettablePromise() {
	}

	public static <T> SettablePromise<T> ofPromise(Promise<T> promise) {
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
	 * Sets the result of this {@code SettablePromise} and completes it.
	 * <p>AssertionError is thrown when you try to set result for  already completed promise.</p>
	 */
	public void set(@Nullable T result) {
		assert !isComplete();
		this.result = result;
		this.exception = null;
		complete(result);
	}

	public void set(@Nullable T result, @Nullable Throwable throwable) {
		if (throwable == null) {
			set(result);
		} else {
			setException(throwable);
		}
	}

	/**
	 * Sets exception and completes this {@code SettablePromise} exceptionally.
	 * <p>AssertionError is thrown when you try to set exception for  already completed promise.</p>
	 *
	 * @param throwable exception
	 */
	public void setException(Throwable throwable) {
		assert throwable != null;
		assert !isComplete();
		result = null;
		exception = throwable;
		completeExceptionally(throwable);
	}

	/**
	 * The same as {@link SettablePromise#trySet(Object, Throwable)} )} but for result only.
	 */
	public void trySet(@Nullable T result) {
		if (isComplete()) {
			return;
		}
		set(result);
	}

	/**
	 * Tries to set result or exception for this {@code SettablePromise} if it not yet set.
	 * <p>Otherwise do nothing</p>
	 *
	 * @return {@code true} if result or exception was set, {@code false} otherwise
	 */
	public void trySet(@Nullable T result, @Nullable Throwable throwable) {
		if (isComplete()) {
			return;
		}
		if (throwable == null) {
			trySet(result);
		} else {
			trySetException(throwable);
		}
	}

	/**
	 * The same as {@link SettablePromise#trySet(Object, Throwable)} )} but for exception only.
	 */
	public void trySetException(Throwable throwable) {
		assert throwable != null;
		if (isComplete()) {
			return;
		}
		setException(throwable);
	}

	public void post(@Nullable T result) {
		getCurrentEventloop().post(() -> set(result));
	}

	public void postException(Throwable throwable) {
		getCurrentEventloop().post(() -> setException(throwable));
	}

	public void post(@Nullable T result, @Nullable Throwable throwable) {
		getCurrentEventloop().post(() -> set(result, throwable));
	}

	public void tryPost(@Nullable T result) {
		getCurrentEventloop().post(() -> trySet(result));
	}

	public void tryPostException(Throwable throwable) {
		assert throwable != null;
		getCurrentEventloop().post(() -> trySetException(throwable));
	}

	public void tryPost(@Nullable T result, @Nullable Throwable throwable) {
		getCurrentEventloop().post(() -> trySet(result, throwable));
	}

	@Override
	protected void subscribe(BiConsumer<? super T, Throwable> next) {
		assert !isComplete();
		super.subscribe(next);
	}

	@Override
	public T getResult() {
		if (isResult()) {
			return result;
		}
		throw new IllegalStateException();
	}

	@Override
	public Throwable getException() {
		if (isException()) {
			return exception;
		}
		throw new IllegalStateException();
	}

	@Override
	public Try<T> asTry() {
		return isComplete() ? Try.of(result, exception) : null;
	}

	@Override
	public boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		if (isComplete()) {
			consumer.accept(result, exception);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean setResultTo(Consumer<? super T> consumer) {
		if (isResult()) {
			consumer.accept(result);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean setExceptionTo(Consumer<Throwable> consumer) {
		if (isException()) {
			consumer.accept(exception);
			return true;
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public <U> Promise<U> mold() {
		assert isException() : "Trying to mold a successful SettablePromise!";
		return (Promise<U>) this;
	}

	@Override
	public <U, S extends BiConsumer<? super T, Throwable> & Promise<U>> Promise<U> then(S promise) {
		if (isComplete()) {
			promise.accept(result, exception);
			return promise;
		}
		return super.then(promise);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> thenApply(Function<? super T, ? extends U> fn) {
		if (isComplete()) {
			try {
				return isResult() ? Promise.of(fn.apply(result)) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenApply(fn);
	}

	@Override
	public <U> Promise<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		if (isComplete()) {
			try {
				return Promise.of(fn.apply(result, exception));
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenApplyEx(fn);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> thenCompose(Function<? super T, ? extends Promise<U>> fn) {
		if (isComplete()) {
			try {
				return isResult() ? fn.apply(result) : (Promise<U>) this;
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenCompose(fn);
	}

	@Override
	public <U> Promise<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Promise<U>> fn) {
		if (isComplete()) {
			try {
				return fn.apply(result, exception);
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}
		return super.thenComposeEx(fn);
	}

	@Override
	public Promise<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		if (isComplete()) {
			action.accept(result, exception);
			return this;
		}
		return super.whenComplete(action);
	}

	@Override
	public Promise<T> whenResult(Consumer<? super T> action) {
		if (isComplete()) {
			if (isResult()) action.accept(result);
			return this;
		}
		return super.whenResult(action);
	}

	@Override
	public Promise<T> whenException(Consumer<Throwable> action) {
		if (isComplete()) {
			if (isException()) action.accept(exception);
			return this;
		}
		return super.whenException(action);
	}

	@Override
	public Promise<T> thenException(Function<? super T, Throwable> fn) {
		if (isComplete()) {
			if (isResult()) {
				Throwable maybeException = fn.apply(result);
				if (maybeException == null) return Promise.of(result);
				return Promise.ofException(maybeException);
			} else {
				return this;
			}
		}
		return super.thenException(fn);
	}

	@Override
	public MaterializedPromise<T> async() {
		if (isComplete()) {
			SettablePromise<T> result = new SettablePromise<>();
			getCurrentEventloop().post(isResult() ?
					() -> result.set(this.result) :
					() -> result.setException(exception));
			return result;
		}
		return this;
	}

	@Override
	public Promise<Try<T>> toTry() {
		if (isComplete()) {
			return Promise.of(Try.of(result, exception));
		}
		return super.toTry();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Promise<Void> toVoid() {
		if (isComplete()) {
			return isResult() ? Promise.complete() : (Promise<Void>) this;
		}
		return super.toVoid();
	}

	@Override
	public <U, V> Promise<V> combine(Promise<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		if (isComplete()) {
			return Promise.of(result, exception).combine(other, fn);
		}
		return super.combine(other, fn);
	}

	@Override
	public Promise<Void> both(Promise<?> other) {
		if (isComplete()) {
			return Promise.of(result, exception).both(other);
		}
		return super.both(other);
	}

	@Override
	public Promise<T> either(Promise<? extends T> other) {
		if (isComplete()) {
			return Promise.of(result, exception).either(other);
		}
		return super.either(other);
	}

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
