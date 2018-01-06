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
import io.datakernel.async.Stages;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.async.SettableStage.mirrorOf;
import static io.datakernel.async.Stages.onError;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class StreamProducers {
	private StreamProducers() {
	}

	/**
	 * Returns producer which doing nothing - not sending any data and not closing itself.
	 */
	public static <T> StreamProducer<T> idle() {
		return new IdleImpl<>();
	}

	/**
	 * Returns producer which closes itself
	 *
	 * @param <T> type of item for send
	 */
	public static <T> StreamProducer<T> closing() {
		return new EndOfStreamImpl<>();
	}

	public static <T> StreamProducer<T> closingWithError(Throwable t) {
		return new ClosingWithErrorImpl<>(t);
	}

	public static <T> StreamProducer<T> endOfStreamOnError(StreamProducer<T> producer) {
		return new StreamProducerDecorator<T>(producer) {
			@Override
			protected void onCloseWithError(Throwable t) {
				sendEndOfStream();
			}
		};
	}

	public static <T> StreamProducer<T> noEndOfStream(StreamProducer<T> producer) {
		return new StreamProducerDecorator<T>(producer) {
			@Override
			protected void onEndOfStream() {
				// do nothing
			}
		};
	}

	/**
	 * Creates producer which sends value and closes itself
	 *
	 * @param value value for sending
	 * @param <T>   type of value
	 */
	public static <T> StreamProducer<T> of(T value) {
		return new OfValueImpl<>(value);
	}

	public static <T> StreamProducer<T> of(T... values) {
		return ofIterable(asList(values));
	}

	/**
	 * Returns new {@link OfIteratorImpl} which sends items from iterator
	 *
	 * @param iterator iterator with items for sending
	 * @param <T>      type of item
	 */
	public static <T> StreamProducer<T> ofIterator(Iterator<T> iterator) {
		return new OfIteratorImpl<>(iterator);
	}

	/**
	 * Returns new {@link OfIteratorImpl} which sends items from {@code iterable}
	 *
	 * @param iterable iterable with items for sending
	 * @param <T>      type of item
	 */
	public static <T> StreamProducer<T> ofIterable(Iterable<T> iterable) {
		return new OfIteratorImpl<>(iterable.iterator());
	}

	public static <T> StreamProducer<T> ofStage(CompletionStage<StreamProducer<T>> producerStage) {
		return new StreamProducerDecorator<T>() {
			{
				producerStage.whenCompleteAsync((producer, throwable) -> {
					if (throwable == null) {
						setActualProducer(producer);
					} else {
						setActualProducer(StreamProducers.closingWithError(throwable));
					}
				});
			}
		};
	}

	public static <T, X> StreamProducerWithResult<T, X> ofStageWithResult(CompletionStage<StreamProducerWithResult<T, X>> producerStage) {
		SettableStage<X> result = SettableStage.create();
		return withResult(new StreamProducerDecorator<T>() {
			{
				producerStage.whenCompleteAsync((producer, throwable) -> {
					if (throwable == null) {
						setActualProducer(producer);
						producer.getResult().whenComplete(result::set);
					} else {
						setActualProducer(StreamProducers.closingWithError(throwable));
						result.setException(throwable);
					}
				});
			}
		}, result);
	}

	public static <T> StreamProducerWithResult<T, Void> withEndOfStreamAsResult(StreamProducer<T> producer) {
		return new StreamProducerWithResult<T, Void>() {
			@Override
			public void setConsumer(StreamConsumer<T> consumer) {
				producer.setConsumer(consumer);
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				producer.produce(dataReceiver);
			}

			@Override
			public void suspend() {
				producer.suspend();
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return producer.getEndOfStream();
			}

			@Override
			public CompletionStage<Void> getResult() {
				return producer.getEndOfStream();
			}
		};
	}

	public static <T, X> StreamProducerWithResult<T, X> withResult(StreamProducer<T> producer, CompletionStage<X> result) {
		SettableStage<X> safeResult = mirrorOf(result);
		producer.getEndOfStream().whenComplete(onError(safeResult::trySetException));
		return new StreamProducerWithResult<T, X>() {
			@Override
			public void setConsumer(StreamConsumer<T> consumer) {
				producer.setConsumer(consumer);
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				producer.produce(dataReceiver);
			}

			@Override
			public void suspend() {
				producer.suspend();
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return producer.getEndOfStream();
			}

			@Override
			public CompletionStage<X> getResult() {
				return safeResult;
			}
		};
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from Iterator  which will stream to this
	 *
	 * @param iterator iterator with producers
	 * @param <T>      type of output data
	 */
	public static <T> StreamProducer<T> concat(Iterator<StreamProducer<T>> iterator) {
		return new StreamProducerConcat<>(iterator);
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from Iterable which will stream to this
	 *
	 * @param producers list of producers
	 * @param <T>       type of output data
	 */
	public static <T> StreamProducer<T> concat(List<StreamProducer<T>> producers) {
		return concat(producers.iterator());
	}

	@SafeVarargs
	public static <T> StreamProducer<T> concat(StreamProducer<T>... producers) {
		return concat(asList(producers));
	}

	/**
	 * Represent a {@link AbstractStreamProducer} which once sends to consumer end of stream.
	 *
	 * @param <T>
	 */
	static class EndOfStreamImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		EndOfStreamImpl() {
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			getCurrentEventloop().post(() -> endOfStream.trySet(null));
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			// do nothing
		}

		@Override
		public void suspend() {
			// do nothing
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
		}
	}

	/**
	 * Represent producer which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	static class ClosingWithErrorImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		private final Throwable exception;

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			getCurrentEventloop().post(() -> endOfStream.trySetException(exception));
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			// do nothing
		}

		@Override
		public void suspend() {
			// do nothing
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
		}
	}

	static final class IdleImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			consumer.getEndOfStream()
					.whenComplete(Stages.onError(endOfStream::trySetException));
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
		}

		@Override
		public void suspend() {
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
		}
	}

	/**
	 * Represents a {@link AbstractStreamProducer} which will send all values from iterator.
	 *
	 * @param <T> type of output data
	 */
	static class OfIteratorImpl<T> extends AbstractStreamProducer<T> {
		private final Iterator<T> iterator;

		/**
		 * Creates a new instance of  StreamProducerOfIterator
		 *
		 * @param iterator iterator with object which need to send
		 */
		public OfIteratorImpl(Iterator<T> iterator) {
			this.iterator = checkNotNull(iterator);
		}

		@Override
		protected void produce() {
			for (; ; ) {
				if (!iterator.hasNext())
					break;
				StreamDataReceiver<T> dataReceiver = getCurrentDataReceiver();
				if (dataReceiver == null)
					return;
				T item = iterator.next();
				dataReceiver.onData(item);
			}
			sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
		}
	}

	/**
	 * It is {@link AbstractStreamProducer} which sends specified single value to its consumer, followed by end-of-stream
	 *
	 * @param <T> type of value for send
	 */
	static class OfValueImpl<T> extends AbstractStreamProducer<T> {
		private final T value;

		/**
		 * Creates producer which sends value and closes itself
		 *
		 * @param value value for sending
		 */
		public OfValueImpl(T value) {
			this.value = value;
		}

		@Override
		protected void produce() {
			send(value);
			sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
		}
	}

	public static <T> StreamProducer<T> errorDecorator(StreamProducer<T> producer, Predicate<T> errorPredicate, Supplier<Throwable> error) {
		return new StreamProducerDecorator<T>(producer) {
			@Override
			protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
				return super.onProduce(item -> {
					if (errorPredicate.test(item)) {
						this.closeWithError(error.get());
					} else {
						dataReceiver.onData(item);
					}
				});
			}
		};
	}

	public static <T, R> StreamProducerWithResult<T, R> errorDecorator(StreamProducerWithResult<T, R> producer, Predicate<T> errorPredicate, Supplier<Throwable> error) {
		return withResult(errorDecorator((StreamProducer<T>) producer, errorPredicate, error), producer.getResult());
	}

}
