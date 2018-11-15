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

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.exception.UncheckedException;
import io.datakernel.serial.SerialConsumer;

import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Consumer;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final Throwable exception;
		private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		ClosingWithErrorImpl(Throwable e) {
			this.exception = e;
		}

		@Override
		public void setSupplier(StreamSupplier<T> supplier) {
			getCurrentEventloop().post(() -> acknowledgement.trySetException(exception));
		}

		@Override
		public MaterializedPromise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void close(Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	static final class OfConsumerImpl<T> extends AbstractStreamConsumer<T> {
		private final Consumer<T> consumer;

		OfConsumerImpl(Consumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			assert getSupplier() != null;
			getSupplier().resume(item -> {
				try {
					consumer.accept(item);
				} catch (UncheckedException u) {
					close(u.getCause());
				}
			});
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			try {
				consumer.accept(null);
				return Promise.complete();
			} catch (UncheckedException u) {
				return Promise.ofException(u.getCause());
			}
		}

		@Override
		protected void onError(Throwable e) {
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	static final class OfSerialConsumerImpl<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private final SerialConsumer<T> consumer;
		private final ArrayDeque<T> deque = new ArrayDeque<>();
		private final SettablePromise<Void> result = new SettablePromise<>();
		private boolean writing;

		OfSerialConsumerImpl(SerialConsumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			produce();
		}

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
		protected Promise<Void> onEndOfStream() {
			produce();
			return Promise.complete();
		}

		private void produce() {
			if (writing) return;
			while (!deque.isEmpty()) {
				Promise<Void> accept = consumer.accept(deque.poll());
				if (accept.isResult()) continue;
				writing = true;
				accept.whenComplete(($, e) -> {
					writing = false;
					if (e == null) {
						produce();
					} else {
						close(e);
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
		protected void onError(Throwable e) {
			deque.clear();
			consumer.close(e);
			result.trySetException(e);
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
		private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void setSupplier(StreamSupplier<T> supplier) {
			supplier.getEndOfStream().whenComplete(acknowledgement::trySet);
		}

		@Override
		public MaterializedPromise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void close(Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	static final class Skip<T> implements StreamConsumer<T> {
		private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void setSupplier(StreamSupplier<T> supplier) {
			supplier.getEndOfStream().whenComplete(acknowledgement::trySet);
			supplier.resume($ -> {});
		}

		@Override
		public MaterializedPromise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void close(Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

}
