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

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This interface represents asynchronous consumer that consumes data items.
 */
@FunctionalInterface
public interface AsyncConsumer<T> {
	/**
	 * Asynchronous operation to consume some data
	 *
	 * @param value value to be consumed
	 * @return {@link Promise} of {@link Void} that represents succesful consumption of data
	 */
	Promise<Void> accept(T value);

	/**
	 * Wrapper around standard Java's {@link Consumer} interface.
	 *
	 * @param consumer - Java's {@link Consumer} of Promises
	 * @return {@link AsyncSupplier} that works on top of standard Java's {@link Supplier} interface
	 */

	static <T> AsyncConsumer<T> of(Consumer<? super T> consumer) {
		return value -> {
			consumer.accept(value);
			return Promise.complete();
		};
	}

	default <R> R transformWith(Function<AsyncConsumer<T>, R> fn) {
		return fn.apply(this);
	}

	default AsyncConsumer<T> async() {
		return value -> accept(value).async();
	}

	default AsyncConsumer<T> withExecutor(AsyncExecutor asyncExecutor) {
		return value -> asyncExecutor.execute(() -> accept(value));
	}

	default <V> AsyncConsumer<V> map(Function<? super V, ? extends T> fn) {
		return value -> accept(fn.apply(value));
	}

	default <V> AsyncConsumer<V> mapAsync(Function<? super V, ? extends Promise<T>> fn) {
		return value -> fn.apply(value).thenCompose(this::accept);
	}

	default AsyncConsumer<T> whenException(Consumer<Throwable> action) {
		return value -> accept(value).whenException(action);
	}
}
