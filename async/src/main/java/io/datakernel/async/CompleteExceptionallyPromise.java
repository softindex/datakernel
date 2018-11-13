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

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class CompleteExceptionallyPromise<T> implements MaterializedPromise<T> {
	private final Throwable exception;

	public CompleteExceptionallyPromise(Throwable exception) {
		this.exception = exception;
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

	@Override
	public Throwable getException() {
		return exception;
	}

	@Override
	public Try<T> asTry() {
		return Try.ofException(exception);
	}

	@Override
	public boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		consumer.accept(null, exception);
		return true;
	}

	@Override
	public boolean setResultTo(Consumer<? super T> consumer) {
		return false;
	}

	@Override
	public boolean setExceptionTo(Consumer<Throwable> consumer) {
		consumer.accept(exception);
		return true;
	}

	@SuppressWarnings("unchecked")
	public <U> CompleteExceptionallyPromise<U> mold() {
		return (CompleteExceptionallyPromise<U>) this;
	}

	@Override
	public <U, S extends BiConsumer<? super T, Throwable> & Promise<U>> Promise<U> then(S promise) {
		promise.accept(null, exception);
		return promise;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> thenApply(Function<? super T, ? extends U> fn) {
		return (CompleteExceptionallyPromise<U>) this;
	}

	@Override
	public <U> Promise<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(null, exception));
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Promise<U> thenCompose(Function<? super T, ? extends Promise<U>> fn) {
		return (CompleteExceptionallyPromise<U>) this;
	}

	@Override
	public <U> Promise<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Promise<U>> fn) {
		try {
			return fn.apply(null, exception);
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@Override
	public Promise<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		action.accept(null, exception);
		return this;
	}

	@Override
	public Promise<T> whenResult(Consumer<? super T> action) {
		return this;
	}

	@Override
	public Promise<T> whenException(Consumer<Throwable> action) {
		action.accept(exception);
		return this;
	}

	@Override
	public Promise<T> thenException(Function<? super T, Throwable> fn) {
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U, V> Promise<V> combine(Promise<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		return (CompleteExceptionallyPromise<V>) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Promise<Void> both(Promise<?> other) {
		return (CompleteExceptionallyPromise<Void>) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Promise<T> either(Promise<? extends T> other) {
		return (Promise<T>) other;
	}

	@Override
	public MaterializedPromise<T> async() {
		SettablePromise<T> result = new SettablePromise<>();
		getCurrentEventloop().post(() -> result.setException(exception));
		return result;
	}

	@Override
	public Promise<Try<T>> toTry() {
		return Promise.of(Try.ofException(exception));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Promise<Void> toVoid() {
		return (CompleteExceptionallyPromise<Void>) this;
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(exception);
		return future;
	}
}
