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
import io.datakernel.async.Stage;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.util.ThrowingConsumer;

import java.util.EnumSet;
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
			getCurrentEventloop().post(() -> endOfStream.trySetException(exception));
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

	static final class OfConsumerImpl<T> extends AbstractStreamConsumer<T> {
		private final ThrowingConsumer<T> consumer;

		OfConsumerImpl(ThrowingConsumer<T> consumer) {
			this.consumer = consumer;
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
			try {
				consumer.accept(null);
			} catch (RuntimeException e) {
				throw e;
			} catch (Throwable ignored) {
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

	static final class OfSerialConsumerImpl<T> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, Void>, StreamDataReceiver<T> {
		private final SerialConsumer<T> consumer;
		private int waiting;
		private final SettableStage<Void> resultStage = new SettableStage<>();

		OfSerialConsumerImpl(SerialConsumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onData(T item) {
			assert item != null;
			Stage<Void> stage = consumer.accept(item);
			if (stage.isSet()) {
				if (stage.isResult()) {
					return;
				} else {
					closeWithError(stage.getException());
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
								consumer.accept(null)
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
				consumer.accept(null)
						.whenComplete(resultStage::trySet);
			}
		}

		@Override
		protected void onError(Throwable t) {
			consumer.closeWithError(t);
			resultStage.trySetException(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public MaterializedStage<Void> getResult() {
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
			producer.getEndOfStream().whenComplete(endOfStream::trySet);
			producer.produce($ -> {});
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

}
