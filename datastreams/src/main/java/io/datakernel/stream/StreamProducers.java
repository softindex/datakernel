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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings("StatementWithEmptyBody")
public final class StreamProducers {
	private StreamProducers() {}

	/**
	 * Represent producer which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	static class ClosingWithErrorImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		private final Throwable exception;

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			getCurrentEventloop().post(() -> endOfStream.setException(exception));
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
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	/**
	 * Represent producer which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	static class ClosingImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			getCurrentEventloop().post(() -> endOfStream.set(null));
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
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	static final class IdleImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			consumer.getEndOfStream()
					.whenException(endOfStream::setException);
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
		}

		@Override
		public void suspend() {
		}

		@Override
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
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
		protected void produce(AsyncProduceController async) {
			while (iterator.hasNext()) {
				StreamDataReceiver<T> dataReceiver = getCurrentDataReceiver();
				if (dataReceiver == null) {
					return;
				}
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
			return EnumSet.of(LATE_BINDING);
		}
	}

	static class OfAsyncSupplierImpl<T> extends AbstractStreamProducer<T> {
		private final AsyncSupplier<T> asyncCallable;
		private final Object endOfStreamMarker;

		/**
		 * Creates a new instance of  StreamProducerOfIterator
		 *
		 * @param asyncCallable iterator with object which need to send
		 * @param endOfStreamMarker
		 */
		public OfAsyncSupplierImpl(AsyncSupplier<T> asyncCallable, Object endOfStreamMarker) {
			this.asyncCallable = checkNotNull(asyncCallable);
			this.endOfStreamMarker = endOfStreamMarker;
		}

		@Override
		protected void produce(AsyncProduceController async) {
			async.begin();
			asyncCallable.get()
					.whenComplete((value, e) -> {
						if (e == null) {
							if (value != endOfStreamMarker) {
								send(value);
								async.resume();
							} else {
								sendEndOfStream();
							}
						} else {
							closeWithError(e);
						}
					});
		}

		@Override
		protected void onError(Throwable t) {
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	public static <T> StreamProducerModifier<T, T> endOfStreamOnError(Predicate<Throwable> endOfStreamPredicate) {
		return producer -> new ForwardingStreamProducer<T>(producer) {
			final SettableStage<Void> endOfStream = new SettableStage<>();

			{
				producer.getEndOfStream().whenComplete(($, throwable) -> {
					if (throwable == null) {
						endOfStream.set(null);
					} else {
						if (endOfStreamPredicate.test(throwable)) {
							endOfStream.set(null);
						} else {
							endOfStream.setException(throwable);
						}
					}
				});
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamProducerModifier<T, T> endOfStreamOnError() {
		return endOfStreamOnError(throwable -> true);
	}

	public static <T> StreamProducerModifier<T, T> noEndOfStream() {
		return producer -> new ForwardingStreamProducer<T>(producer) {
			final SettableStage<Void> endOfStream = new SettableStage<>();

			{
				producer.getEndOfStream()
						.whenException(endOfStream::setException);
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

}
