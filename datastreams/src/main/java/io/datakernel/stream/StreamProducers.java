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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.stream.StreamCapability.TERMINAL;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class StreamProducers {
	private StreamProducers() {
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

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING, TERMINAL);
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

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING, TERMINAL);
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

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING, TERMINAL);
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
		return errorDecorator((StreamProducer<T>) producer, errorPredicate, error).withResult(producer.getResult());
	}


}
