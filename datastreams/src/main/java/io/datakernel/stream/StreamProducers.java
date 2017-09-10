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

import io.datakernel.eventloop.Eventloop;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.async.AsyncCallbacks.throwableToException;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamStatus.SUSPENDED;

public final class StreamProducers {
	private StreamProducers() {
	}

	/**
	 * Returns producer which doing nothing - not sending any data and not closing itself.
	 */
	public static <T> StreamProducer<T> idle() {
		return new Idle<>();
	}

	/**
	 * Returns producer which closes itself
	 *
	 * @param eventloop event loop in which will run it
	 * @param <T>       type of item for send
	 */
	public static <T> StreamProducer<T> closing(Eventloop eventloop) {
		return new EndOfStream<>(eventloop);
	}

	public static <T> StreamProducer<T> closingWithError(Eventloop eventloop, Exception e) {
		return new ClosingWithError<>(eventloop, e);
	}

	public static <T> StreamProducer<T> closingOnError(StreamProducer<T> producer) {
		return new StreamProducerDecorator<T>(producer) {
			@Override
			protected void onCloseWithError(Exception e) {
				getConsumer().endOfStream();
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
	 * @param eventloop event loop in which will run it
	 * @param value     value for sending
	 * @param <T>       type of value
	 */
	public static <T> StreamProducer<T> ofValue(Eventloop eventloop, final T value) {
		return new OfValue<>(eventloop, value);
	}

	/**
	 * Creates producer which sends value and closes itself
	 *
	 * @param eventloop event loop in which will run it
	 * @param value     value for sending
	 * @param close     if producer is closed
	 * @param <T>       type of value
	 */
	public static <T> StreamProducer<T> ofValue(Eventloop eventloop, final T value, boolean close) {
		return new OfValue<>(eventloop, value, close);
	}

	/**
	 * Returns new {@link OfIterator} which sends items from iterator
	 *
	 * @param eventloop event loop in which will run it
	 * @param iterator  iterator with items for sending
	 * @param <T>       type of item
	 */
	public static <T> StreamProducer<T> ofIterator(Eventloop eventloop, Iterator<T> iterator) {
		return new OfIterator<>(eventloop, iterator);
	}

	/**
	 * Returns new {@link OfIterator} which sends items from {@code iterable}
	 *
	 * @param eventloop event loop in which will run it
	 * @param iterable  iterable with items for sending
	 * @param <T>       type of item
	 */
	public static <T> StreamProducer<T> ofIterable(Eventloop eventloop, Iterable<T> iterable) {
		return new OfIterator<>(eventloop, iterable.iterator());
	}

	public static <T> StreamProducer<T> ofStage(CompletionStage<StreamProducer<T>> producerStage) {
		StreamProducerDecorator<T> decorator = StreamProducerDecorator.create();
		producerStage
				.exceptionally(throwable ->
						closingWithError(getCurrentEventloop(), throwableToException(throwable)))
				.thenAccept(decorator::setActualProducer);
		return decorator;
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from Iterator  which will stream to this
	 *
	 * @param eventloop event loop in which will run it
	 * @param iterator  iterator with producers
	 * @param <T>       type of output data
	 */
	public static <T> StreamProducer<T> concat(Eventloop eventloop, Iterator<StreamProducer<T>> iterator) {
		return new StreamProducerConcat<>(eventloop, iterator);
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from Iterable which will stream to this
	 *
	 * @param eventloop event loop in which will run it
	 * @param producers list of producers
	 * @param <T>       type of output data
	 */
	public static <T> StreamProducer<T> concat(Eventloop eventloop, List<StreamProducer<T>> producers) {
		return concat(eventloop, producers.iterator());
	}

	@SafeVarargs
	public static <T> StreamProducer<T> concat(Eventloop eventloop, StreamProducer<T>... producers) {
		return concat(eventloop, Arrays.asList(producers));
	}

	/**
	 * Represent a {@link AbstractStreamProducer} which once sends to consumer end of stream.
	 *
	 * @param <T>
	 */
	public static class EndOfStream<T> extends AbstractStreamProducer<T> {
		public EndOfStream(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void produce() {
			sendEndOfStream();
		}

		@Override
		protected void onError(Exception e) {
		}
	}

	/**
	 * Represent producer which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	public static class ClosingWithError<T> extends AbstractStreamProducer<T> {
		private final Exception exception;

		public ClosingWithError(Eventloop eventloop, Exception exception) {
			super(eventloop);
			this.exception = exception;
		}

		@Override
		protected void produce() {
			closeWithError(exception);
		}

		@Override
		protected void onError(Exception e) {
		}
	}

	public static final class Idle<T> implements StreamProducer<T> {
		private StreamConsumer<T> consumer;
		private StreamDataReceiver<T> dataReceiver;

		@Override
		public void streamTo(StreamConsumer<T> consumer) {
			checkNotNull(consumer);
			if (this.consumer == consumer) return;
			checkState(this.consumer == null);

			this.consumer = consumer;

			consumer.streamFrom(this);
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			this.dataReceiver = dataReceiver;
		}

		@Override
		public void suspend() {
			this.dataReceiver = null;
		}

		@Override
		public void closeWithError(Exception e) {
			consumer.closeWithError(e);
		}

		public StreamConsumer<T> getConsumer() {
			return consumer;
		}

		public StreamDataReceiver<T> getDataReceiver() {
			return dataReceiver;
		}
	}

	/**
	 * Represents a {@link AbstractStreamProducer} which will send all values from iterator.
	 *
	 * @param <T> type of output data
	 */
	public static class OfIterator<T> extends AbstractStreamProducer<T> {
		private final Iterator<T> iterator;

		/**
		 * Creates a new instance of  StreamProducerOfIterator
		 *
		 * @param eventloop event loop where producer will run
		 * @param iterator  iterator with object which need to send
		 */
		public OfIterator(Eventloop eventloop, Iterator<T> iterator) {
			super(eventloop);
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
		protected void onError(Exception e) {
		}
	}

	/**
	 * It is {@link AbstractStreamProducer} which sends specified single value to its consumer, followed by end-of-stream
	 *
	 * @param <T> type of value for send
	 */
	public static class OfValue<T> extends AbstractStreamProducer<T> {
		private final T value;
		private final boolean sendEndOfStream;

		/**
		 * Creates producer which sends value and closes itself
		 *
		 * @param eventloop event loop  in which this producer will run
		 * @param value     value for sending
		 */
		public OfValue(Eventloop eventloop, T value) {
			this(eventloop, value, true);
		}

		/**
		 * Creates producer which sends value and optionally closes itself
		 *
		 * @param eventloop       event loop  in which this producer will run
		 * @param value           value for sending
		 * @param sendEndOfStream if producer is closed
		 */
		public OfValue(Eventloop eventloop, T value, boolean sendEndOfStream) {
			super(eventloop);
			this.value = value;
			this.sendEndOfStream = sendEndOfStream;
		}

		@Override
		protected void produce() {
			send(value);
			if (sendEndOfStream)
				sendEndOfStream();
		}

		@Override
		protected void onError(Exception e) {
		}
	}

	/**
	 * Represents {@link AbstractStreamTransformer_1_1}, which created with iterator with {@link AbstractStreamProducer}
	 * which will stream to this
	 *
	 * @param <T> type of received data
	 */
	public static class StreamProducerConcat<T> extends AbstractStreamProducer<T> {
		private final Iterator<StreamProducer<T>> iterator;
		private StreamProducer<T> producer;

		public StreamProducerConcat(Eventloop eventloop, Iterator<StreamProducer<T>> iterator) {
			super(eventloop);
			this.iterator = iterator;
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			assert dataReceiver != null;
			if (producer == null) {
				if (!iterator.hasNext()) {
					eventloop.post(this::sendEndOfStream);
					return;
				}
				producer = iterator.next();
				producer.streamTo(new AbstractStreamConsumer<T>(eventloop) {
					@Override
					protected void onEndOfStream() {
						eventloop.post(() -> {
							producer = null;
							if (isReceiverReady()) {
								onProduce(getCurrentDataReceiver());
							}
						});
					}

					@Override
					protected void onError(Exception e) {
						StreamProducerConcat.this.closeWithError(e);
					}
				});
			}
			producer.produce(dataReceiver);
		}

		@Override
		protected void onSuspended() {
			if (producer != null) {
				producer.suspend();
			}
		}

		@Override
		protected void onError(Exception e) {
			if (producer != null) {
				producer.closeWithError(e);
			}
		}

		@Override
		protected void cleanup() {
			producer = null;
		}
	}
}
