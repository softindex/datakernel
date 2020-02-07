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

import io.datakernel.async.process.Cancellable;
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
 * It represents object for asynchronous sending streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of output data
 */
public interface StreamSupplier<T> extends Cancellable {
	/**
	 * This method is called for restore streaming of this supplier
	 */
	void supply(@Nullable StreamDataAcceptor<T> dataAcceptor);

	Promise<Void> getEndOfStream();

	default Promise<Void> streamTo(@NotNull StreamConsumer<T> consumer) {
		this.getEndOfStream()
				.whenResult(consumer::endOfStream)
				.whenException(consumer::closeEx);
		consumer.getAcknowledgement()
				.whenResult(this::close)
				.whenException(this::closeEx);
		if (!this.getEndOfStream().isComplete() && !consumer.getAcknowledgement().isComplete()) {
			consumer.consume(this::supply);
		}
		return Promises.all(this.getEndOfStream(), consumer.getAcknowledgement());
	}

	default Promise<Void> streamTo(Promise<StreamConsumer<T>> consumerPromise) {
		return streamTo(StreamConsumer.ofPromise(consumerPromise));
	}

	default <X> Promise<X> streamTo(@NotNull StreamConsumerWithResult<T, X> consumerWithResult) {
		return this.streamTo(consumerWithResult.getConsumer())
				.then(consumerWithResult::getResult);
	}

	static <T> StreamSupplier<T> ofConsumer(Consumer<StreamConsumer<T>> consumer) {
		StreamTransformer<T, T> forwarder = StreamTransformer.identity();
		consumer.accept(forwarder.getInput());
		return forwarder.getOutput();
	}

	/**
	 * Returns supplier which doing nothing - not sending any data and not closing itself.
	 */
	static <T> StreamSupplier<T> idle() {
		return new Idle<>();
	}

	/**
	 * Returns supplier which only closes itself.
	 */
	static <T> StreamSupplier<T> closing() {
		return new Closing<>();
	}

	/**
	 * Returns supplier which only closes itself with given error.
	 */
	static <T> StreamSupplier<T> closingWithError(Throwable e) {
		return new ClosingWithError<>(e);
	}

	/**
	 * Creates supplier which sends values and closes itself
	 *
	 * @param values values for sending
	 * @param <T>    type of value
	 */
	@SafeVarargs
	static <T> StreamSupplier<T> of(T... values) {
		return new OfIterator<>(asList(values).iterator());
	}

	/**
	 * Returns new {@link OfIterator} which sends items from iterator
	 *
	 * @param iterator iterator with items for sending
	 * @param <T>      type of item
	 */
	static <T> StreamSupplier<T> ofIterator(Iterator<T> iterator) {
		return new OfIterator<>(iterator);
	}

	/**
	 * Returns new {@link OfIterator} which sends items from {@code iterable}
	 *
	 * @param iterable iterable with items for sending
	 * @param <T>      type of item
	 */
	static <T> StreamSupplier<T> ofIterable(Iterable<T> iterable) {
		return new OfIterator<>(iterable.iterator());
	}

	static <T> StreamSupplier<T> ofStream(Stream<T> stream) {
		return new OfIterator<>(stream.iterator());
	}

	/**
	 * Creates a stream supplier which produces items from a given lambda.
	 * End of stream is marked as null, so no null values cannot be used.
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

	static <T> StreamSupplier<T> ofChannelSupplier(ChannelSupplier<T> supplier) {
		return new StreamSuppliers.OfChannelSupplier<>(supplier);
	}

	default <R> R transformWith(StreamSupplierTransformer<T, R> fn) {
		return fn.transform(this);
	}

	static <T> StreamSupplier<T> ofPromise(Promise<? extends StreamSupplier<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new StreamSuppliers.OfPromise<>(promise);
	}

	/**
	 * Returns  {@link StreamSupplierConcat} with suppliers from Iterator  which will stream to this
	 *
	 * @param iterator iterator with suppliers
	 * @param <T>      type of output data
	 */
	static <T> StreamSupplier<T> concat(Iterator<StreamSupplier<T>> iterator) {
		return new StreamSupplierConcat<>(ChannelSupplier.ofIterator(iterator));
	}

	/**
	 * Returns  {@link StreamSupplierConcat} with suppliers from Iterable which will stream to this
	 *
	 * @param suppliers list of suppliers
	 * @param <T>       type of output data
	 */
	static <T> StreamSupplier<T> concat(List<StreamSupplier<T>> suppliers) {
		return concat(suppliers.iterator());
	}

	@SafeVarargs
	static <T> StreamSupplier<T> concat(StreamSupplier<T>... suppliers) {
		return concat(asList(suppliers));
	}

	default Promise<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	default <A, R> Promise<R> toCollector(Collector<T, A, R> collector) {
		StreamConsumerToCollector<T, A, R> consumerToCollector = new StreamConsumerToCollector<>(collector);
		this.streamTo(consumerToCollector);
		return consumerToCollector.getResult();
	}

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
