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

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.util.ThrowingConsumer;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumers {
	private StreamConsumers() {}

	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final Throwable exception;
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setProducer(StreamProducer<T> producer) {
			getCurrentEventloop().post(() -> endOfStream.setException(exception));
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

	static final class OfConsumerImpl<T> extends AbstractStreamConsumer<T> {
		private static final Object NO_END_OF_STREAM_MARKER = new Object();
		private final ThrowingConsumer<T> consumer;
		private final Object endOfStreamMarker;

		OfConsumerImpl(ThrowingConsumer<T> consumer) {
			this.consumer = consumer;
			this.endOfStreamMarker = NO_END_OF_STREAM_MARKER;
		}

		OfConsumerImpl(ThrowingConsumer<T> consumer, Object endOfStreamMarker) {
			this.consumer = consumer;
			this.endOfStreamMarker = endOfStreamMarker;
		}

		@Override
		protected void onStarted() {
			assert getProducer() != null;
			getProducer().produce(t -> {
				try {
					consumer.accept(t);
				} catch (RuntimeException e) {
					throw e;
				} catch (Throwable e) {
					closeWithError(e);
				}
			});
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void onEndOfStream() {
			if (endOfStreamMarker != NO_END_OF_STREAM_MARKER) {
				try {
					consumer.accept((T) endOfStreamMarker);
				} catch (RuntimeException e) {
					throw e;
				} catch (Throwable ignored) {
				}
			}
		}

		@Override
		protected void onError(Throwable t) {

		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	static final class OfAsyncConsumerImpl<T> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, Void>, StreamDataReceiver<T> {
		private static final Object NO_END_OF_STREAM_MARKER = new Object();
		private final AsyncConsumer<T> consumer;
		private final Object endOfStreamMarker;
		private int waiting;
		private final SettableStage<Void> resultStage = new SettableStage<>();

		OfAsyncConsumerImpl(AsyncConsumer<T> consumer) {
			this.consumer = consumer;
			this.endOfStreamMarker = NO_END_OF_STREAM_MARKER;
		}

		OfAsyncConsumerImpl(AsyncConsumer<T> consumer, Object endOfStreamMarker) {
			this.consumer = consumer;
			this.endOfStreamMarker = endOfStreamMarker;
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onData(T item) {
			assert item != endOfStreamMarker;
			Stage<Void> stage = consumer.accept(item);
			if (stage instanceof SettableStage) {
				SettableStage<Void> settableStage = (SettableStage<Void>) stage;
				if (settableStage.isSetResult()) {
					// do nothing, continue streaming
					return;
				} else if (settableStage.isSetException()) {
					closeWithError(settableStage.getException());
					return;
				}
			}
			waiting++;
			getProducer().suspend();
			stage.whenComplete(($, throwable) -> {
				if (--waiting == 0) {
					if (throwable == null) {
						if (getStatus().isOpen()) {
							getProducer().streamTo(this);
						} else {
							if (getStatus() == StreamStatus.END_OF_STREAM) {
								(endOfStreamMarker != NO_END_OF_STREAM_MARKER ? consumer.accept((T) endOfStreamMarker) : Stage.of((Void) null))
										.whenComplete(resultStage::trySet);
							}
						}
					} else {
						closeWithError(throwable);
					}
				}
			});
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void onEndOfStream() {
			if (waiting == 0) {
				(endOfStreamMarker != NO_END_OF_STREAM_MARKER ? consumer.accept((T) endOfStreamMarker) : Stage.of((Void) null))
						.whenComplete(resultStage::trySet);
			}
		}

		@Override
		protected void onError(Throwable t) {
			resultStage.trySetException(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public Stage<Void> getResult() {
			return resultStage;
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	static final class IdleImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		@Override
		public void setProducer(StreamProducer<T> producer) {
			producer.getEndOfStream().whenComplete(endOfStream::set);
			producer.produce($ -> {});
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

}
