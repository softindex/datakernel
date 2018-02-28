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
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
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
		private final SettableStage<Void> endOfStream = SettableStage.create();

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

	static final class IdleImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

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
			return EnumSet.of(LATE_BINDING);
		}
	}

	public static <T> StreamProducerModifier<T, T> suppliedEndOfStream(Function<Stage<Void>, Stage<Void>> endOfStreamSupplier) {
		return producer -> new ForwardingStreamProducer<T>(producer) {
			final Stage<Void> endOfStream = endOfStreamSupplier.apply(producer.getEndOfStream());

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamProducerModifier<T, T> suppliedEndOfStream(Stage<Void> suppliedEndOfStream) {
		return suppliedEndOfStream(actualEndOfStream -> Stages.any(actualEndOfStream, suppliedEndOfStream));
	}

	public interface Decorator<T> {
		interface Context {
			void endOfStream();

			void closeWithError(Throwable error);
		}

		StreamDataReceiver<T> decorate(Context context, StreamDataReceiver<T> dataReceiver);
	}

	public static <T> StreamProducerModifier<T, T> decorator(Decorator<T> decorator) {
		return producer -> new ForwardingStreamProducer<T>(producer) {
			final SettableStage<Void> endOfStream = SettableStage.create();

			{
				producer.getEndOfStream().whenComplete(endOfStream::trySet);
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				producer.produce(decorator.decorate(new Decorator.Context() {
					@Override
					public void endOfStream() {
						endOfStream.trySet(null);
					}

					@Override
					public void closeWithError(Throwable error) {
						endOfStream.trySetException(error);
					}
				}, dataReceiver));
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamProducerModifier<T, T> errorDecorator(Function<T, Throwable> errorFunction) {
		return decorator((context, dataReceiver) ->
				item -> {
					Throwable error = errorFunction.apply(item);
					if (error == null) {
						dataReceiver.onData(item);
					} else {
						context.closeWithError(error);
					}
				});
	}

	public static <T> StreamProducerModifier<T, T> endOfStreamOnError(Predicate<Throwable> endOfStreamPredicate) {
		return producer -> new ForwardingStreamProducer<T>(producer) {
			final SettableStage<Void> endOfStream = SettableStage.create();

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
			final SettableStage<Void> endOfStream = SettableStage.create();

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
