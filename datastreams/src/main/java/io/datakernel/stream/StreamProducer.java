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

import io.datakernel.async.SettableStage;
import io.datakernel.util.Modifier;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.SettableStage.mirrorOf;
import static io.datakernel.async.Stages.onError;
import static io.datakernel.stream.DataStreams.stream;
import static java.util.Arrays.asList;

/**
 * It represents object for asynchronous sending streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of output data
 */
public interface StreamProducer<T> extends Modifier<StreamProducer<T>> {
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

	CompletionStage<Void> getEndOfStream();

	/**
	 * Returns producer which doing nothing - not sending any data and not closing itself.
	 */
	static <T> StreamProducer<T> idle() {
		return new StreamProducers.IdleImpl<>();
	}

	static <T> StreamProducer<T> closingWithError(Throwable t) {
		return new StreamProducers.ClosingWithErrorImpl<>(t);
	}

	/**
	 * Creates producer which sends values and closes itself
	 *
	 * @param values values for sending
	 * @param <T>    type of value
	 */
	static <T> StreamProducer<T> of(T... values) {
		return ofIterable(asList(values));
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

	static <T> StreamProducer<T> ofStage(CompletionStage<StreamProducer<T>> producerStage) {
		return new StreamProducerDecorator<T>() {
			{
				producerStage.whenCompleteAsync((producer, throwable) -> {
					if (throwable == null) {
						setActualProducer(producer);
					} else {
						setActualProducer(StreamProducer.closingWithError(throwable));
					}
				});
			}
		};
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

	default <X> StreamProducerWithResult<T, X> withResult(CompletionStage<X> result) {
		SettableStage<X> safeResult = mirrorOf(result);
		getEndOfStream().whenComplete(onError(safeResult::trySetException));
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
			public CompletionStage<Void> getEndOfStream() {
				return StreamProducer.this.getEndOfStream();
			}

			@Override
			public CompletionStage<X> getResult() {
				return safeResult;
			}
		};
	}

	default StreamProducerWithResult<T, Void> withEndOfStreamAsResult() {
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
			public CompletionStage<Void> getEndOfStream() {
				return StreamProducer.this.getEndOfStream();
			}

			@Override
			public CompletionStage<Void> getResult() {
				return StreamProducer.this.getEndOfStream();
			}
		};
	}

	default StreamProducer<T> endOfStreamOnError() {
		return new StreamProducerDecorator<T>(this) {
			@Override
			protected void onCloseWithError(Throwable t) {
				sendEndOfStream();
			}
		};
	}

	default StreamProducer<T> noEndOfStream() {
		return new StreamProducerDecorator<T>(this) {
			@Override
			protected void onEndOfStream() {
				// do nothing
			}
		};
	}

	default CompletionStage<List<T>> getList() {
		return stream(this, new StreamConsumerToList<>());
	}

}
