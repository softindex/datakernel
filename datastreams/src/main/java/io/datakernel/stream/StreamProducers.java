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

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;
import io.datakernel.serial.SerialSupplier;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings("StatementWithEmptyBody")
public final class StreamProducers {
	private StreamProducers() {
	}

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
		public MaterializedStage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void closeWithError(Throwable e) {
			endOfStream.trySetException(e);
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
		public MaterializedStage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void closeWithError(Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	static final class IdleImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			consumer.getAcknowledgement()
					.whenException(endOfStream::trySetException);
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
		}

		@Override
		public void suspend() {
		}

		@Override
		public MaterializedStage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void closeWithError(Throwable e) {
			endOfStream.trySetException(e);
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

	static class OfSerialSupplierImpl<T> extends AbstractStreamProducer<T> {
		private final SerialSupplier<T> supplier;

		public OfSerialSupplierImpl(SerialSupplier<T> supplier) {
			this.supplier = supplier;
		}

		@Override
		protected void produce(AsyncProduceController async) {
			async.begin();
			supplier.get()
					.whenComplete((value, e) -> {
						if (e == null) {
							if (value != null) {
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
			supplier.closeWithError(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	public static <T> StreamProducerModifier<T, T> endOfStreamOnError() {
		return endOfStreamOnError(e -> true);
	}

	public static <T> StreamProducerModifier<T, T> endOfStreamOnError(Predicate<Throwable> endOfStreamPredicate) {
		return producer -> new ForwardingStreamProducer<T>(producer) {
			final SettableStage<Void> endOfStream = new SettableStage<>();

			{
				producer.getEndOfStream().whenComplete(($, throwable) -> {
					if (throwable == null) {
						endOfStream.trySet(null);
					} else {
						if (endOfStreamPredicate.test(throwable)) {
							endOfStream.trySet(null);
						} else {
							endOfStream.trySetException(throwable);
						}
					}
				});
			}

			@Override
			public MaterializedStage<Void> getEndOfStream() {
				return endOfStream;
			}

			@Override
			public void closeWithError(Throwable e) {
				super.closeWithError(e);
				endOfStream.trySetException(e);
			}
		};
	}

}
