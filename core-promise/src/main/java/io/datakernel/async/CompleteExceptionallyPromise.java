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

import io.datakernel.exception.UncheckedException;
import io.datakernel.functional.Try;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Represents a {@code Promise} which is completed with an exception.
 */
public final class CompleteExceptionallyPromise<T> implements MaterializedPromise<T> {
	@NotNull
	private final Throwable exception;

	public CompleteExceptionallyPromise(@NotNull Throwable e) {
		this.exception = e;
	}

	@Override
	public boolean isComplete() {
		return true;
	}

	@Override
	public boolean isResult() {
		return false;
	}

	@Override
	public boolean isException() {
		return true;
	}

	@Override
	public T getResult() {
		throw new UnsupportedOperationException();
	}

	@NotNull
	@Override
	public Throwable getException() {
		return exception;
	}

	@NotNull
	@Override
	public Try<T> getTry() {
		return Try.ofException(exception);
	}

	@NotNull
	@Override
	public <U, S extends BiConsumer<? super T, Throwable> & Promise<U>> Promise<U> next(@NotNull S promise) {
		promise.accept(null, exception);
		return promise;
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> map(@NotNull Function<? super T, ? extends U> fn) {
		return (CompleteExceptionallyPromise<U>) this;
	}

	@NotNull
	@Override
	public <U> Promise<U> mapEx(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(null, exception));
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> then(@NotNull Function<? super T, ? extends Promise<U>> fn) {
		return (CompleteExceptionallyPromise<U>) this;
	}

	@NotNull
	@Override
	public <U> Promise<U> thenEx(@NotNull BiFunction<? super T, Throwable, ? extends Promise<U>> fn) {
		try {
			return fn.apply(null, exception);
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@NotNull
	@Override
	public Promise<T> acceptEx(@NotNull BiConsumer<? super T, Throwable> action) {
		action.accept(null, exception);
		return this;
	}

	@NotNull
	@Override
	public Promise<T> accept(@NotNull Consumer<? super T> action) {
		return this;
	}

	@Override
	public Promise<T> acceptEx(Class<? extends Throwable> type, @NotNull Consumer<Throwable> action) {
		if (type.isAssignableFrom(exception.getClass())) {
			action.accept(exception);
		}
		return this;
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public <U, V> Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunction<? super T, ? super U, ? extends V> fn) {
		return (CompleteExceptionallyPromise<V>) this;
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public Promise<Void> both(@NotNull Promise<?> other) {
		return (CompleteExceptionallyPromise<Void>) this;
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public Promise<T> either(@NotNull Promise<? extends T> other) {
		return (Promise<T>) other;
	}

	@NotNull
	@Override
	public MaterializedPromise<T> async() {
		SettablePromise<T> result = new SettablePromise<>();
		getCurrentEventloop().post(() -> result.setException(exception));
		return result;
	}

	@NotNull
	@Override
	public Promise<Try<T>> toTry() {
		return Promise.of(Try.ofException(exception));
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public Promise<Void> toVoid() {
		return (CompleteExceptionallyPromise<Void>) this;
	}

	@NotNull
	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(exception);
		return future;
	}
}
