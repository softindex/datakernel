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

package io.datakernel.datastream;

import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.StreamSuppliers.Closing;
import io.datakernel.datastream.StreamSuppliers.ClosingWithError;
import io.datakernel.datastream.StreamSuppliers.Idle;
import io.datakernel.datastream.StreamSuppliers.OfIterator;
import io.datakernel.datastream.processor.StreamTransformer;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 * This interface represents an object that can asynchronously send streams of data.
 * <p>
 * Implementors of this interface might want to extend {@link AbstractStreamSupplier}
 * instead of this interface, since it makes the threading and state management easier.
 */
public interface StreamSupplier<T> extends AsyncCloseable {

	/**
	 * Sets the {@link StreamDataAcceptor} to which this supplies must stream its data.
	 * <p>
	 * Calling this with <code>null</code> parameter is the same as calling {@link #suspend()}
	 * and all suspension rules apply.
	 * <p>
	 * This method must not be called after {@link #getEndOfStream() the end of stream} is set.
	 */
	void resume(@Nullable StreamDataAcceptor<T> dataAcceptor);

	/**
	 * Must put the supplier in a <i>suspended</i> state. Supplier in a suspended state
	 * <b>must not</b> send data to any {@link StreamDataAcceptor} that it may have got
	 * from the {@link #resume} method.
	 * <p>
	 * This method must not be called after {@link #getEndOfStream() the end of stream} is set.
	 */
	default void suspend() {
		resume(null);
	}

	/**
	 * A signal promise of the <i>end of stream</i> state of this supplier - its completion means that
	 * this supplier changed to that state and is now <b>closed</b>.
	 * <p>
	 * In this state supplier <b>must not</b> supply anything to any acceptors (just like when suspended),
	 * and {@link #resume} and {@link #suspend} methods must never be called.
	 * <p>
	 * If promise completes with an error then this supplier closes with that error.
	 */
	Promise<Void> getEndOfStream();

	/**
	 * Bind this supplier to given {@link StreamConsumer} and start streaming
	 * data through them following all the contracts.
	 */
	default Promise<Void> streamTo(@NotNull StreamConsumer<T> consumer) {
		consumer.getAcknowledgement()
				.whenResult(this::close)
				.whenException(this::closeEx);
		consumer.consume(this);
		return Promises.all(this.getEndOfStream(), consumer.getAcknowledgement());
	}

	/**
	 * A shortcut for {@link #streamTo(StreamConsumer)} that uses a promise of a stream.
	 */
	default Promise<Void> streamTo(Promise<StreamConsumer<T>> consumerPromise) {
		return streamTo(StreamConsumer.ofPromise(consumerPromise));
	}

	/**
	 * A shortcut for {@link #streamTo(StreamConsumer)} for {@link StreamConsumerWithResult}.
	 */
	default <X> Promise<X> streamTo(@NotNull StreamConsumerWithResult<T, X> consumerWithResult) {
		return this.streamTo(consumerWithResult.getConsumer())
				.then(consumerWithResult::getResult);
	}

	/**
	 * Creates a supplier which supplies items that were sent into the consumer received through the callback.
	 */
	static <T> StreamSupplier<T> ofConsumer(Consumer<StreamConsumer<T>> consumer) {
		StreamTransformer<T, T> forwarder = StreamTransformer.identity();
		consumer.accept(forwarder.getInput());
		return forwarder.getOutput();
	}

	/**
	 * Creates a supplier which does not send any data and never moves to the closed state.
	 */
	static <T> StreamSupplier<T> idle() {
		return new Idle<>();
	}

	/**
	 * Creates a supplier that is in the closed state immediately.
	 */
	static <T> StreamSupplier<T> closing() {
		return new Closing<>();
	}

	/**
	 * Creates a supplier that is in the closed state with given error set.
	 */
	static <T> StreamSupplier<T> closingWithError(Throwable e) {
		return new ClosingWithError<>(e);
	}

	/**
	 * Creates a supplier which supplies given items and then closes.
	 */
	@SafeVarargs
	static <T> StreamSupplier<T> of(T... items) {
		return new OfIterator<>(asList(items).iterator());
	}

	/**
	 * Creates a supplier which supplies items from the given iterator and then closes.
	 */
	static <T> StreamSupplier<T> ofIterator(Iterator<T> iterator) {
		return new OfIterator<>(iterator);
	}

	/**
	 * Creates a supplier which supplies items from the given iterable and then closes.
	 */
	static <T> StreamSupplier<T> ofIterable(Iterable<T> iterable) {
		return new OfIterator<>(iterable.iterator());
	}

	/**
	 * Creates a supplier which supplies items from the given stream and then closes.
	 */
	static <T> StreamSupplier<T> ofStream(Stream<T> stream) {
		return new OfIterator<>(stream.iterator());
	}

	/**
	 * Creates a supplier which supplies items by calling a given lambda.
	 * It closes itself (and changes to closed state) when lambda returns <code>null</code>.
	 */
	static <T> StreamSupplier<T> ofSupplier(Supplier<T> supplier) {
		return new OfIterator<>(new Iterator<T>() {
			private T next = supplier.get();

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public T next() {
				T n = next;
				next = supplier.get();
				return n;
			}
		});
	}

	/**
	 * Creates a supplier which supplies items from the given channel supplier and then closes.
	 */
	static <T> StreamSupplier<T> ofChannelSupplier(ChannelSupplier<T> supplier) {
		return new StreamSuppliers.OfChannelSupplier<>(supplier);
	}

	/**
	 * Creates a supplier that waits until the promise completes
	 * and then supplies items from the resulting supplier.
	 */
	static <T> StreamSupplier<T> ofPromise(Promise<? extends StreamSupplier<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new StreamSuppliers.OfPromise<>(promise);
	}

	/**
	 * Transforms this supplier with a given transformer.
	 */
	default <R> R transformWith(StreamSupplierTransformer<T, R> fn) {
		return fn.transform(this);
	}

	/**
	 * Creates a supplier that supplies items from given suppliers consecutively and only then closes.
	 */
	static <T> StreamSupplier<T> concat(Iterator<StreamSupplier<T>> iterator) {
		return new StreamSupplierConcat<>(ChannelSupplier.ofIterator(iterator));
	}

	/**
	 * A shortcut for {@link #concat(Iterator)} that uses a list of suppliers
	 */
	static <T> StreamSupplier<T> concat(List<StreamSupplier<T>> suppliers) {
		return concat(suppliers.iterator());
	}

	/**
	 * A shortcut for {@link #concat(Iterator)} that uses given suppliers
	 */
	@SafeVarargs
	static <T> StreamSupplier<T> concat(StreamSupplier<T>... suppliers) {
		return concat(asList(suppliers));
	}

	/**
	 * Accumulates items from this supplier until it closes and
	 * then completes the returned promise with the accumulator.
	 */
	default <A, R> Promise<R> toCollector(Collector<T, A, R> collector) {
		StreamConsumerToCollector<T, A, R> consumerToCollector = new StreamConsumerToCollector<>(collector);
		this.streamTo(consumerToCollector);
		return consumerToCollector.getResult();
	}

	/**
	 * A shortcut for {@link #toCollector} that accumulates to a {@link List}.
	 */
	default Promise<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	/**
	 * Creates a supplier from this one with its <i>end of stream</i> signal modified by the given function.
	 */
	default StreamSupplier<T> withEndOfStream(Function<Promise<Void>, Promise<Void>> fn) {
		Promise<Void> endOfStream = getEndOfStream();
		Promise<Void> suppliedEndOfStream = fn.apply(endOfStream);
		if (endOfStream == suppliedEndOfStream) {
			return this;
		}
		return new ForwardingStreamSupplier<T>(this) {
			@Override
			public Promise<Void> getEndOfStream() {
				return suppliedEndOfStream;
			}
		};
	}

}
