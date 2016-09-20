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

import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.async.AsyncIterators.asyncIteratorOfIterator;

public final class StreamProducers {
	private StreamProducers() {
	}

	/**
	 * Returns producer which doing nothing - not sending any data and not closing itself.
	 *
	 * @param eventloop event loop in which will run it
	 */
	public static <T> StreamProducer<T> idle(Eventloop eventloop) {
		return new Idle<>(eventloop);
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

	/**
	 * Represents asynchronously resolving producer.
	 *
	 * @param eventloop      event loop in which will run it
	 * @param producerGetter getter with producer
	 * @param <T>            type of output data
	 */
	public static <T> StreamProducer<T> asynchronouslyResolving(final Eventloop eventloop, final AsyncGetter<StreamProducer<T>> producerGetter) {
		final StreamForwarder<T> forwarder = StreamForwarder.create(eventloop);
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				producerGetter.get(new ResultCallback<StreamProducer<T>>() {
					@Override
					public void onResult(StreamProducer<T> actualProducer) {
						actualProducer.streamTo(forwarder.getInput());
					}

					@Override
					public void onException(Exception exception) {
						new ClosingWithError<T>(eventloop, exception).streamTo(forwarder.getInput());
					}
				});
			}
		});
		return forwarder.getOutput();
	}

	/**
	 * Returns {@link StreamProducerConcat} with producers from asyncIterator  which will stream to this
	 *
	 * @param eventloop     event loop in which will run it
	 * @param asyncIterator iterator with producers
	 * @param <T>           type of output data
	 */
	public static <T> StreamProducer<T> concat(Eventloop eventloop, AsyncIterator<StreamProducer<T>> asyncIterator) {
		return new StreamProducerConcat<>(eventloop, asyncIterator);
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from AsyncIterable  which will stream to this
	 *
	 * @param eventloop     event loop in which will run it
	 * @param asyncIterator iterable with producers
	 * @param <T>           type of output data
	 */
	public static <T> StreamProducer<T> concat(Eventloop eventloop, AsyncIterable<StreamProducer<T>> asyncIterator) {
		return concat(eventloop, asyncIterator.asyncIterator());
	}

	/**
	 * Returns  {@link StreamProducerConcat} with producers from Iterator  which will stream to this
	 *
	 * @param eventloop event loop in which will run it
	 * @param iterator  iterator with producers
	 * @param <T>       type of output data
	 */
	public static <T> StreamProducer<T> concat(Eventloop eventloop, Iterator<StreamProducer<T>> iterator) {
		return concat(eventloop, asyncIteratorOfIterator(iterator));
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
		protected void onStarted() {
			sendEndOfStream();
		}

		@Override
		protected void onDataReceiverChanged() {
		}

		@Override
		protected void onSuspended() {

		}

		@Override
		protected void onResumed() {

		}

	}

	/**
	 * Represent producer which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	public static class ClosingWithError<T> extends AbstractStreamProducer<T> {
		private final Logger logger = LoggerFactory.getLogger(this.getClass());
		private final Exception exception;

		public ClosingWithError(Eventloop eventloop, Exception exception) {
			super(eventloop);
			this.exception = exception;
		}

		@Override
		protected void onStarted() {
			logger.trace("{} close with error {}", this, exception.getMessage());
			closeWithError(exception);
		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		protected void onSuspended() {

		}

		@Override
		protected void onResumed() {

		}

	}

	public static class Idle<T> extends AbstractStreamProducer<T> {
		public Idle(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		protected void onSuspended() {

		}

		@Override
		protected void onResumed() {

		}

		@Override
		public void sendEndOfStream() {
			super.sendEndOfStream();
		}
	}

	/**
	 * Represents a {@link AbstractStreamProducer} which will send all values from iterator.
	 *
	 * @param <T> type of output data
	 */
	public static class OfIterator<T> extends AbstractStreamProducer<T> {
		private final Iterator<T> iterator;
		private boolean sendEndOfStream = true;

		/**
		 * Creates a new instance of  StreamProducerOfIterator
		 *
		 * @param eventloop event loop where producer will run
		 * @param iterator  iterator with object which need to send
		 */
		public OfIterator(Eventloop eventloop, Iterator<T> iterator) {
			this(eventloop, iterator, true);
		}

		public OfIterator(Eventloop eventloop, Iterator<T> iterator, boolean sendEndOfStream) {
			super(eventloop);
			this.iterator = checkNotNull(iterator);
			this.sendEndOfStream = sendEndOfStream;
		}

		@Override
		protected void doProduce() {
			for (; ; ) {
				if (!iterator.hasNext())
					break;
				if (!isStatusReady())
					return;
				T item = iterator.next();
				send(item);
			}
			if (sendEndOfStream)
				sendEndOfStream();
		}

		@Override
		protected void onStarted() {
			produce();
		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		protected void onSuspended() {

		}

		@Override
		protected void onResumed() {
			resumeProduce();
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
		protected void doProduce() {
			send(value);
			if (sendEndOfStream)
				sendEndOfStream();
		}

		@Override
		protected void onStarted() {
			produce();
		}

		@Override
		protected void onDataReceiverChanged() {
		}

		@Override
		protected void onSuspended() {

		}

		@Override
		protected void onResumed() {
			resumeProduce();
		}
	}

	/**
	 * Represents {@link AbstractStreamTransformer_1_1}, which created with iterator with {@link AbstractStreamProducer}
	 * which will stream to this
	 *
	 * @param <T> type of received data
	 */
	public static class StreamProducerConcat<T> extends StreamProducerDecorator<T> {
		private final AsyncIterator<StreamProducer<T>> iterator;
		private final ForwarderConcat forwarderConcat;

		public StreamProducerConcat(Eventloop eventloop, AsyncIterator<StreamProducer<T>> iterator) {
			this.forwarderConcat = new ForwarderConcat(eventloop);
			this.iterator = iterator;
			setActualProducer(forwarderConcat.getOutput());
		}

		private class ForwarderConcat extends AbstractStreamTransformer_1_1<T, T> {
			protected InputConsumer inputConsumer;
			protected OutputProducer outputProducer;

			protected ForwarderConcat(Eventloop eventloop) {
				super(eventloop);
				inputConsumer = new InputConsumer();
				outputProducer = new OutputProducer();
			}

			private class InputConsumer extends AbstractInputConsumer {

				private void doNext() {
					eventloop.post(new Runnable() {
						@Override
						public void run() {
							iterator.next(new IteratorCallback<StreamProducer<T>>() {
								@Override
								public void onNext(StreamProducer<T> actualProducer) {
									actualProducer.streamTo(new StreamConsumerDecorator<T>(ForwarderConcat.this.getInput()) {
										@Override
										public void onProducerEndOfStream() {
											inputConsumer.onUpstreamEndOfStream();
										}
									});
								}

								@Override
								public void onEnd() {
									outputProducer.sendEndOfStream();
								}

								@Override
								public void onException(Exception e) {
									closeWithError(e);
								}
							});
						}
					});
				}

				@Override
				protected void onUpstreamEndOfStream() {
					doNext();
				}

				@Override
				public StreamDataReceiver<T> getDataReceiver() {
					return outputProducer.getDownstreamDataReceiver();
				}
			}

			private class OutputProducer extends AbstractOutputProducer {

				@Override
				protected void onDownstreamStarted() {
					inputConsumer.doNext();
				}

				@Override
				protected void onDownstreamSuspended() {
					inputConsumer.suspend();
				}

				@Override
				protected void onDownstreamResumed() {
					inputConsumer.resume();
				}
			}
		}
	}
}
