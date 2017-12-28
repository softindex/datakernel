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
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

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
		StreamProducerDecorator<T, Void> decorator = new StreamProducerDecorator<T, Void>() {
			@Override
			protected void onCloseWithError(Throwable t) {
				getConsumer().endOfStream();
			}
		};
		decorator.setActualProducer(producer);
		return decorator;
	}

	public static <T> StreamProducer<T> noEndOfStream(StreamProducer<T> producer) {
		StreamProducerDecorator<T, Void> decorator = new StreamProducerDecorator<T, Void>() {
			@Override
			protected void onEndOfStream() {
				// do nothing
			}
		};
		decorator.setActualProducer(producer);
		return decorator;
	}

	/**
	 * Creates producer which sends value and closes itself
	 *
	 * @param eventloop event loop in which will run it
	 * @param value     value for sending
	 * @param <T>       type of value
	 */
	public static <T> StreamProducer<T> ofValue(Eventloop eventloop, final T value) {
		return new OfValueImpl<>(eventloop, value);
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
		return new OfValueImpl<>(eventloop, value, close);
	}

	/**
	 * Returns new {@link OfIteratorImpl} which sends items from iterator
	 *
	 * @param eventloop event loop in which will run it
	 * @param iterator  iterator with items for sending
	 * @param <T>       type of item
	 */
	public static <T> StreamProducer<T> ofIterator(Eventloop eventloop, Iterator<T> iterator) {
		return new OfIteratorImpl<>(eventloop, iterator);
	}

	/**
	 * Returns new {@link OfIteratorImpl} which sends items from {@code iterable}
	 *
	 * @param eventloop event loop in which will run it
	 * @param iterable  iterable with items for sending
	 * @param <T>       type of item
	 */
	public static <T> StreamProducer<T> ofIterable(Eventloop eventloop, Iterable<T> iterable) {
		return new OfIteratorImpl<>(eventloop, iterable.iterator());
	}

	public static <T> StreamProducer<T> ofStage(CompletionStage<StreamProducer<T>> producerStage) {
		StreamProducerDecorator<T, Void> decorator = new StreamProducerDecorator<T, Void>() {};
		producerStage.whenCompleteAsync((producer, throwable) -> {
			if (throwable == null) {
				decorator.setActualProducer(producer);
			} else {
				decorator.setActualProducer(StreamProducers.closingWithError(throwable));
			}
		});
		return decorator;
	}

	public static <T, X> StreamProducerWithResult<T, X> ofStageWithResult(CompletionStage<StreamProducerWithResult<T, X>> producerStage) {
		StreamProducerDecorator<T, X> decorator = new StreamProducerDecorator<T, X>() {};
		producerStage.whenCompleteAsync((producer, throwable) -> {
			if (throwable == null) {
				decorator.setActualProducer(producer, producer.getResult());
			} else {
				decorator.setActualProducer(StreamProducers.closingWithError(throwable));
			}
		});
		return decorator;
	}

	public static <T> StreamProducer<T> onEndOfStream(StreamProducer<T> producer, BiConsumer<Void, Throwable> consumer) {
		final StreamProducerWithResult<T, Void> withEndOfStream = withEndOfStream(producer);
		withEndOfStream.getResult().whenComplete(consumer);
		return withEndOfStream;
	}

	public static <T> StreamProducerWithResult<T, Void> withEndOfStream(StreamProducer<T> producer) {
		if (producer instanceof HasEndOfStream) {
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
				public void closeWithError(Throwable t) {
					producer.closeWithError(t);
				}

				@Override
				public CompletionStage<Void> getResult() {
					return ((HasEndOfStream) producer).getEndOfStream();
				}
			};
		}
		StreamProducerDecorator<T, Void> decorator = new StreamProducerDecorator<T, Void>() {};
		decorator.setActualProducer(producer, decorator.getEndOfStream());
		return decorator;
	}

	public static <T, X> StreamProducerWithResult<T, X> withResult(StreamProducer<T> producer, CompletionStage<X> result) {
		StreamProducerDecorator<T, X> decorator = new StreamProducerDecorator<T, X>() {};
		decorator.setActualProducer(producer, result);
		return decorator;
	}

	/**
	 * Returns  {@link StreamProducerConcatImpl} with producers from Iterator  which will stream to this
	 *
	 * @param eventloop event loop in which will run it
	 * @param iterator  iterator with producers
	 * @param <T>       type of output data
	 */
	public static <T> StreamProducer<T> concat(Eventloop eventloop, Iterator<StreamProducer<T>> iterator) {
		return new StreamProducerConcatImpl<>(eventloop, iterator);
	}

	/**
	 * Returns  {@link StreamProducerConcatImpl} with producers from Iterable which will stream to this
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
	static class EndOfStreamImpl<T> implements StreamProducer<T> {
		EndOfStreamImpl() {
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			getCurrentEventloop().post(consumer::endOfStream);
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
		public void closeWithError(Throwable t) {
		}
	}

	/**
	 * Represent producer which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	static class ClosingWithErrorImpl<T> implements StreamProducer<T> {
		private final Throwable exception;

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			getCurrentEventloop().post(() -> {
				consumer.closeWithError(exception);
			});
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
		public void closeWithError(Throwable t) {
			// do nothing
		}
	}

	static final class IdleImpl<T> implements StreamProducer<T> {
		private StreamConsumer<T> consumer;
		private StreamDataReceiver<T> dataReceiver;

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			checkNotNull(consumer);
			if (this.consumer == consumer) return;
			checkState(this.consumer == null);

			this.consumer = consumer;
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
		public void closeWithError(Throwable t) {
			consumer.closeWithError(t);
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
	static class OfIteratorImpl<T> extends AbstractStreamProducer<T> {
		private final Iterator<T> iterator;

		/**
		 * Creates a new instance of  StreamProducerOfIterator
		 *
		 * @param eventloop event loop where producer will run
		 * @param iterator  iterator with object which need to send
		 */
		public OfIteratorImpl(Eventloop eventloop, Iterator<T> iterator) {
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
		private final boolean sendEndOfStream;

		/**
		 * Creates producer which sends value and closes itself
		 *
		 * @param eventloop event loop  in which this producer will run
		 * @param value     value for sending
		 */
		public OfValueImpl(Eventloop eventloop, T value) {
			this(eventloop, value, true);
		}

		/**
		 * Creates producer which sends value and optionally closes itself
		 *
		 * @param eventloop       event loop  in which this producer will run
		 * @param value           value for sending
		 * @param sendEndOfStream if producer is closed
		 */
		public OfValueImpl(Eventloop eventloop, T value, boolean sendEndOfStream) {
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
		protected void onError(Throwable t) {
		}
	}

	/**
	 * Represents {@link AbstractStreamTransformer_1_1}, which created with iterator with {@link AbstractStreamProducer}
	 * which will stream to this
	 *
	 * @param <T> type of received data
	 */
	static class StreamProducerConcatImpl<T> extends AbstractStreamProducer<T> {
		private final Iterator<StreamProducer<T>> iterator;
		private StreamProducer<T> producer;

		StreamProducerConcatImpl(Eventloop eventloop, Iterator<StreamProducer<T>> iterator) {
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
					protected void onError(Throwable t) {
						StreamProducerConcatImpl.this.closeWithError(t);
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
		protected void onError(Throwable t) {
			if (producer != null) {
				producer.closeWithError(t);
			}
		}

		@Override
		protected void cleanup() {
			producer = null;
		}
	}

	public static <T> StreamProducer<T> errorDecorator(StreamProducer<T> producer, Predicate<T> errorPredicate, Supplier<Throwable> error) {
		final StreamProducerDecorator<T, Void> streamProducerDecorator = new StreamProducerDecorator<T, Void>() {
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
		streamProducerDecorator.setActualProducer(producer);
		return streamProducerDecorator;
	}

	public static <T, R> StreamProducerWithResult<T, R> errorDecorator(StreamProducerWithResult<T, R> producer, Predicate<T> errorPredicate, Supplier<Throwable> error) {
		final StreamProducerDecorator<T, R> streamProducerDecorator = new StreamProducerDecorator<T, R>() {
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
		streamProducerDecorator.setActualProducer(producer, producer.getResult());
		return streamProducerDecorator;
	}

}
