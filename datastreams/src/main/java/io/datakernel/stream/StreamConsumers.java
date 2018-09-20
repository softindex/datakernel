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

import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Set;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final Throwable exception;
		private final SettableStage<Void> acknowledgement = new SettableStage<>();

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setSupplier(StreamSupplier<T> supplier) {
			getCurrentEventloop().post(() -> acknowledgement.trySetException(exception));
		}

		@Override
		public MaterializedStage<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void closeWithError(Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	static final class OfConsumerImpl<T> extends AbstractStreamConsumer<T> {
		private final ThrowingConsumer<T> consumer;

		OfConsumerImpl(ThrowingConsumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			assert getSupplier() != null;
			getSupplier().resume(t -> {
				try {
					consumer.accept(t);
				} catch (RuntimeException e) {
					throw e;
				} catch (Throwable e) {
					closeWithError(e);
				}
			});
		}

		@Override
		protected Stage<Void> onEndOfStream() {
			try {
				consumer.accept(null);
				return Stage.complete();
			} catch (RuntimeException e) {
				throw e;
			} catch (Throwable e) {
				return Stage.ofException(e);
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

	static final class OfSerialConsumerImpl<T> extends AbstractStreamConsumer<T> implements StreamConsumer<T>, StreamDataAcceptor<T> {
		private final SerialConsumer<T> consumer;
		private final ArrayDeque<T> deque = new ArrayDeque<>();
		private final SettableStage<Void> result = new SettableStage<>();
		private boolean writing;

		OfSerialConsumerImpl(SerialConsumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			produce();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void accept(T item) {
			assert item != null;
			if (!deque.isEmpty()) {
				getSupplier().suspend();
			}
			deque.add(item);
			produce();
		}

		@Override
		protected Stage<Void> onEndOfStream() {
			produce();
			return Stage.complete();
		}

		private void produce() {
			if (writing) return;
			while (!deque.isEmpty()) {
				Stage<Void> accept = consumer.accept(deque.poll());
				if (accept.isResult()) continue;
				writing = true;
				accept.whenComplete(($, e) -> {
					writing = false;
					if (e == null) {
						produce();
					} else {
						closeWithError(e);
					}
				});
				return;
			}
			if (getEndOfStream().isResult()) {
				consumer.accept(null)
						.whenComplete(result::trySet);
			} else {
				getSupplier().resume(this);
			}
		}

		@Override
		protected void onError(Throwable t) {
			deque.clear();
			consumer.closeWithError(t);
			result.trySetException(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing supplier sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	static final class Idle<T> implements StreamConsumer<T> {
		private final SettableStage<Void> acknowledgement = new SettableStage<>();

		@Override
		public void setSupplier(StreamSupplier<T> supplier) {
			supplier.getEndOfStream().whenComplete(acknowledgement::trySet);
		}

		@Override
		public MaterializedStage<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void closeWithError(Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	static final class Skip<T> implements StreamConsumer<T> {
		private final SettableStage<Void> acknowledgement = new SettableStage<>();

		@Override
		public void setSupplier(StreamSupplier<T> supplier) {
			supplier.getEndOfStream().whenComplete(acknowledgement::trySet);
			supplier.resume($ -> {});
		}

		@Override
		public MaterializedStage<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void closeWithError(Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

}
