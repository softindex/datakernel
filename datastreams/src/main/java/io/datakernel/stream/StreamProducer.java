/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.stream;

import io.datakernel.async.Cancellable;
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.serial.AbstractSerialSupplier;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.StreamLateBinder;
import io.datakernel.stream.processor.StreamTransformer;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;

/**
 * It represents object for asynchronous sending streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of output data
 */
public interface StreamProducer<T> extends Cancellable {
	/**
	 * Changes consumer for this producer, removes itself from previous consumer and removes
	 * previous producer for new consumer. Begins to stream to consumer.
	 *
	 * @param consumer consumer for streaming
	 */
	void setConsumer(StreamConsumer<T> consumer);

	/**
	 * This method is called for restore streaming of this producer
	 */
	void produce(StreamDataAcceptor<T> dataAcceptor);

	/**
	 * This method is called for stop streaming of this producer
	 */
	void suspend();

	MaterializedStage<Void> getEndOfStream();

	Set<StreamCapability> getCapabilities();

	@SuppressWarnings("unchecked")
	default Stage<Void> streamTo(StreamConsumer<T> consumer) {
		StreamProducer<T> producer = this;
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
		return Stages.all(producer.getEndOfStream(), consumer.getAcknowledgement());
	}

	static <T, R> StreamProducer<T> ofConsumer(Consumer<StreamConsumer<T>> consumer) {
		StreamTransformer<T, T> forwarder = StreamTransformer.identity();
		consumer.accept(forwarder.getInput());
		return forwarder.getOutput();
	}

	/**
	 * Returns producer which doing nothing - not sending any data and not closing itself.
	 */
	static <T> StreamProducer<T> idle() {
		return new StreamProducers.IdleImpl<>();
	}

	/**
	 * Returns producer which only closes itself.
	 */
	static <T> StreamProducer<T> closing() {
		return new StreamProducers.ClosingImpl<>();
	}

	/**
	 * Returns producer which only closes itself with given error.
	 */
	static <T> StreamProducer<T> closingWithError(Throwable t) {
		return new StreamProducers.ClosingWithErrorImpl<>(t);
	}

	/**
	 * Creates producer which sends values and closes itself
	 *
	 * @param values values for sending
	 * @param <T>    type of value
	 */
	@SafeVarargs
	static <T> StreamProducer<T> of(T... values) {
		return new StreamProducers.OfIteratorImpl<>(asList(values).iterator());
	}

	/**
	 * Returns new {@link StreamProducers.OfIteratorImpl} which sends items from iterator
	 *
	 * @param iterator iterator with items for sending
	 * @param <T>      type of item
	 */
	static <T> StreamProducer<T> ofIterator(Iterator<T> iterator) {
		return new StreamProducers.OfIteratorImpl<>(iterator);
	}

	/**
	 * Returns new {@link StreamProducers.OfIteratorImpl} which sends items from {@code iterable}
	 *
	 * @param iterable iterable with items for sending
	 * @param <T>      type of item
	 */
	static <T> StreamProducer<T> ofIterable(Iterable<T> iterable) {
		return new StreamProducers.OfIteratorImpl<>(iterable.iterator());
	}

	static <T> StreamProducer<T> ofStream(Stream<T> stream) {
		return new StreamProducers.OfIteratorImpl<>(stream.iterator());
	}

	/**
	 * Creates a stream producer which produces items from a given lambda.
	 * End of stream is marked as null, so no null values cannot be used.
	 */
	static <T> StreamProducer<T> ofSupplier(Supplier<T> supplier) {
		return new StreamProducers.OfIteratorImpl<>(new Iterator<T>() {
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

	static <T> StreamProducer<T> ofSerialSupplier(SerialSupplier<T> supplier) {
		return new StreamProducers.OfSerialSupplierImpl<>(supplier);
	}

	default <R> R apply(StreamProducerFunction<T, R> fn) {
		return fn.apply(this);
	}

	default StreamProducer<T> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : apply(StreamLateBinder.create());
	}

	@SuppressWarnings("unchecked")
	default SerialSupplier<T> asSerialSupplier() {
		StreamConsumerEndpoint<T> endpoint = new StreamConsumerEndpoint<>();
		this.streamTo(endpoint);
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				return endpoint.take();
			}
		};
	}

	String LATE_BINDING_ERROR_MESSAGE = "" +
			"StreamProducer %s does not have LATE_BINDING capabilities, " +
			"it must be bound in the same tick when it is created. " +
			"Alternatively, use .withLateBinding() modifier";

	static <T> StreamProducer<T> ofStage(Stage<? extends StreamProducer<T>> stage) {
		if (stage.hasResult()) return stage.getResult();
		StreamLateBinder<T> binder = StreamLateBinder.create();
		stage.whenComplete((producer, throwable) -> {
			if (throwable == null) {
				checkArgument(producer.getCapabilities().contains(LATE_BINDING),
						LATE_BINDING_ERROR_MESSAGE, producer);
				producer.streamTo(binder.getInput());
			} else {
				StreamProducer.<T>closingWithError(throwable).streamTo(binder.getInput());
			}
		});
		return binder.getOutput();
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from Iterator  which will stream to this
	 *
	 * @param iterator iterator with producers
	 * @param <T>      type of output data
	 */
	static <T> StreamProducer<T> concat(Iterator<StreamProducer<T>> iterator) {
		return new StreamProducerConcat<>(iterator);
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from Iterable which will stream to this
	 *
	 * @param producers list of producers
	 * @param <T>       type of output data
	 */
	static <T> StreamProducer<T> concat(List<StreamProducer<T>> producers) {
		return concat(producers.iterator());
	}

	@SafeVarargs
	static <T> StreamProducer<T> concat(StreamProducer<T>... producers) {
		return concat(asList(producers));
	}

	default Stage<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	default <A, R> Stage<R> toCollector(Collector<T, A, R> collector) {
		StreamConsumerToCollector<T, A, R> consumerToCollector = new StreamConsumerToCollector<>(collector);
		this.streamTo(consumerToCollector);
		return consumerToCollector.getResult();
	}

	default StreamProducer<T> withEndOfStream(Function<Stage<Void>, Stage<Void>> fn) {
		Stage<Void> endOfStream = getEndOfStream();
		Stage<Void> suppliedEndOfStream = fn.apply(endOfStream);
		if (endOfStream == suppliedEndOfStream) return this;
		MaterializedStage<Void> newEndOfStream = suppliedEndOfStream.materialize();
		return new ForwardingStreamProducer<T>(this) {
			@Override
			public MaterializedStage<Void> getEndOfStream() {
				return newEndOfStream;
			}
		};
	}

}
