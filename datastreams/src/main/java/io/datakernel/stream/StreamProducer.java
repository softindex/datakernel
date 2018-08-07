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

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.stream.processor.StreamLateBinder;
import io.datakernel.stream.processor.StreamSkip;
import io.datakernel.stream.processor.StreamSkip.Dropper;
import io.datakernel.stream.processor.StreamSkip.SizeCounter;
import io.datakernel.stream.processor.StreamSkip.SkipStrategy;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

/**
 * It represents object for asynchronous sending streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of output data
 */
public interface StreamProducer<T> {
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
	void produce(StreamDataReceiver<T> dataReceiver);

	/**
	 * This method is called for stop streaming of this producer
	 */
	void suspend();

	Stage<Void> getEndOfStream();

	Set<StreamCapability> getCapabilities();

	@SuppressWarnings("unchecked")
	default StreamCompletion streamTo(StreamConsumer<T> consumer) {
		StreamProducer<T> producer = this;
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
		Stage<Void> producerEndOfStream = producer.getEndOfStream();
		Stage<Void> consumerEndOfStream = consumer.getEndOfStream();
		Stage<Void> endOfStream = Stages.all(producerEndOfStream, consumerEndOfStream);
		return new StreamCompletion() {
			@Override
			public Stage<Void> getProducerEndOfStream() {
				return producerEndOfStream;
			}

			@Override
			public Stage<Void> getConsumerEndOfStream() {
				return consumerEndOfStream;
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	@SuppressWarnings("unchecked")
	default <Y> StreamConsumerResult<Y> streamTo(StreamConsumerWithResult<T, Y> consumer) {
		StreamProducer<T> producer = this;
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
		Stage<Void> producerEndOfStream = producer.getEndOfStream();
		Stage<Void> consumerEndOfStream = consumer.getEndOfStream();
		Stage<Void> endOfStream = Stages.all(producerEndOfStream, consumerEndOfStream);
		Stage<Y> consumerResult = consumer.getResult();
		return new StreamConsumerResult<Y>() {
			@Override
			public Stage<Y> getConsumerResult() {
				return consumerResult;
			}

			@Override
			public Stage<Void> getProducerEndOfStream() {
				return producerEndOfStream;
			}

			@Override
			public Stage<Void> getConsumerEndOfStream() {
				return consumerEndOfStream;
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	default <R> StreamProducer<R> with(StreamProducerModifier<T, R> modifier) {
		return modifier.applyTo(this);
	}

	default StreamProducer<T> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : with(StreamLateBinder.create());
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

	static <T> StreamProducer<T> ofAsyncSupplier(AsyncSupplier<T> supplier) {
		return new StreamProducers.OfAsyncSupplierImpl<>(supplier);
	}

	String LATE_BINDING_ERROR_MESSAGE = "" +
			"StreamProducer %s does not have LATE_BINDING capabilities, " +
			"it must be bound in the same tick when it is created. " +
			"Alternatively, use .withLateBinding() modifier";

	static <T> StreamProducer<T> ofStage(Stage<? extends StreamProducer<T>> producerStage) {
		StreamLateBinder<T> binder = StreamLateBinder.create();
		producerStage.whenComplete((producer, throwable) -> {
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

	default <X> StreamProducerWithResult<T, X> withResultAsyncSupplier(AsyncSupplier<X> result) {
		return withResult(this.getEndOfStream().thenCompose($ -> result.get()));
	}

	default <X> StreamProducerWithResult<T, X> withResultSupplier(Supplier<X> result) {
		return withResult(this.getEndOfStream().thenCompose($ -> Stage.of(result.get())));
	}

	default <X> StreamProducerWithResult<T, X> withResult(Stage<X> result) {
		SettableStage<Void> safeEndOfStream = new SettableStage<>();
		SettableStage<X> safeResult = new SettableStage<>();
		getEndOfStream().whenComplete(($, throwable) -> {
			safeEndOfStream.trySet($, throwable);
			if (throwable != null) {
				safeResult.trySetException(throwable);
			}
		});
		result.post().whenComplete(safeResult::trySet);
		return new StreamProducerWithResult<T, X>() {
			@Override
			public void setConsumer(StreamConsumer<T> consumer) {
				StreamProducer.this.setConsumer(consumer);
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				StreamProducer.this.produce(dataReceiver);
			}

			@Override
			public void suspend() {
				StreamProducer.this.suspend();
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return safeEndOfStream;
			}

			@Override
			public Stage<X> getResult() {
				return safeResult;
			}

			@Override
			public Set<StreamCapability> getCapabilities() {
				return StreamProducer.this.getCapabilities().contains(LATE_BINDING) ?
						EnumSet.of(LATE_BINDING) : emptySet();
			}
		};
	}

	default StreamProducerWithResult<T, Void> withEndOfStreamAsResult() {
		SettableStage<Void> safeEndOfStream = new SettableStage<>();
		getEndOfStream().post().whenComplete(safeEndOfStream::trySet);
		return new StreamProducerWithResult<T, Void>() {
			@Override
			public void setConsumer(StreamConsumer<T> consumer) {
				StreamProducer.this.setConsumer(consumer);
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				StreamProducer.this.produce(dataReceiver);
			}

			@Override
			public void suspend() {
				StreamProducer.this.suspend();
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return safeEndOfStream;
			}

			@Override
			public Stage<Void> getResult() {
				return safeEndOfStream;
			}

			@Override
			public Set<StreamCapability> getCapabilities() {
				return StreamProducer.this.getCapabilities().contains(LATE_BINDING) ?
						EnumSet.of(LATE_BINDING) : emptySet();
			}
		};
	}

	default Stage<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	default <A, R> Stage<R> toCollector(Collector<T, A, R> collector) {
		return streamTo(StreamConsumerWithResult.toCollector(collector)).getConsumerResult();
	}

	default StreamProducer<T> whenEndOfStream(Runnable runnable) {
		getEndOfStream().whenResult($ -> runnable.run());
		return this;
	}

	default StreamProducer<T> whenException(Consumer<Throwable> consumer) {
		getEndOfStream().whenException(consumer);
		return this;
	}

	default StreamProducer<T> skipFirst(long skip, SizeCounter<T> sizeCounter, Dropper<T> dropper) {
		return with(StreamSkip.create(skip, sizeCounter, dropper));
	}

	default StreamProducer<T> skipFirst(long skip, SkipStrategy<T> strategy) {
		return with(StreamSkip.create(skip, strategy));
	}

	default StreamProducer<T> skipFirst(long skip) {
		return with(StreamSkip.create(skip));
	}
}
