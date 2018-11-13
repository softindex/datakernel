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

public abstract class CompletePromise<T> implements MaterializedPromise<T> {
	@Override
	public final boolean isComplete() {
		return true;
	}

	@Override
	public final boolean isResult() {
		return true;
	}

	@Override
	public final boolean isException() {
		return false;
	}

	@Override
	abstract public T getResult();

	@Override
	public final Throwable getException() {
		throw new UnsupportedOperationException();
	}

	@Override
	public final Try<T> asTry() {
		return Try.of(getResult());
	}

	@Override
	public final boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		consumer.accept(getResult(), null);
		return true;
	}

	@Override
	public final boolean setResultTo(Consumer<? super T> consumer) {
		consumer.accept(getResult());
		return true;
	}

	@Override
	public final boolean setExceptionTo(Consumer<Throwable> consumer) {
		return false;
	}

	@Override
	public final <U, S extends BiConsumer<? super T, Throwable> & Promise<U>> Promise<U> then(S promise) {
		promise.accept(getResult(), null);
		return promise;
	}

	@Override
	public final <U> Promise<U> thenApply(Function<? super T, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult()));
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@Override
	public final <U> Promise<U> thenApplyEx(BiFunction<? super T, Throwable, ? extends U> fn) {
		try {
			return Promise.of(fn.apply(getResult(), null));
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@Override
	public final <U> Promise<U> thenCompose(Function<? super T, ? extends Promise<U>> fn) {
		try {
			return fn.apply(getResult());
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@Override
	public final <U> Promise<U> thenComposeEx(BiFunction<? super T, Throwable, ? extends Promise<U>> fn) {
		try {
			return fn.apply(getResult(), null);
		} catch (UncheckedException u) {
			return Promise.ofException(u.getCause());
		}
	}

	@Override
	public final Promise<T> whenComplete(BiConsumer<? super T, Throwable> action) {
		action.accept(getResult(), null);
		return this;
	}

	@Override
	public final Promise<T> whenResult(Consumer<? super T> action) {
		action.accept(getResult());
		return this;
	}

	@Override
	public final Promise<T> whenException(Consumer<Throwable> action) {
		return this;
	}

	@Override
	public final Promise<T> thenException(Function<? super T, Throwable> fn) {
		Throwable maybeException = fn.apply(getResult());
		if (maybeException == null) return this;
		return Promise.ofException(maybeException);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <U, V> Promise<V> combine(Promise<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
		if (other instanceof CompletePromise) {
			return Promise.of(fn.apply(getResult(), ((CompletePromise<U>) other).getResult()));
		}
		return other.thenApply(otherResult -> fn.apply(getResult(), otherResult));
	}

	@Override
	public final Promise<Void> both(Promise<?> other) {
		if (other instanceof CompletePromise) {
			return Promise.complete();
		}
		return other.toVoid();
	}

	@SuppressWarnings("unchecked")
	@Override
	public final Promise<T> either(Promise<? extends T> other) {
		return this;
	}

	@Override
	public final MaterializedPromise<T> async() {
		SettablePromise<T> result = new SettablePromise<>();
		getCurrentEventloop().post(() -> result.set(getResult()));
		return result;
	}

	@Override
	public final Promise<Try<T>> toTry() {
		return Promise.of(Try.of(getResult()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public final Promise<Void> toVoid() {
		return Promise.complete();
	}

	@Override
	public final CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.complete(getResult());
		return future;
	}
}
