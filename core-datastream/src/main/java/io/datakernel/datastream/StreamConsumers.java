/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.datastream;

import io.datakernel.common.exception.UncheckedException;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

import static io.datakernel.common.Preconditions.checkState;

final class StreamConsumers {

	static final class ClosingWithError<T> implements StreamConsumer<T> {
		private final Promise<Void> acknowledgement;

		ClosingWithError(Throwable e) {
			this.acknowledgement = Promise.ofException(e);
		}

		@Override
		public void consume(@NotNull StreamSupplier<T> streamSupplier) {
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
		}
	}

	static final class OfConsumer<T> implements StreamConsumer<T> {
		private final Consumer<T> consumer;
		private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		OfConsumer(Consumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void consume(@NotNull StreamSupplier<T> streamSupplier) {
			streamSupplier.getEndOfStream()
					.whenResult(acknowledgement::trySet)
					.whenException(this::closeEx);
			if (getAcknowledgement().isComplete()) return;
			streamSupplier.resume(item -> {
				try {
					consumer.accept(item);
				} catch (UncheckedException u) {
					closeEx(u.getCause());
				}
			});
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	static final class Idle<T> implements StreamConsumer<T> {
		private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void consume(@NotNull StreamSupplier<T> streamSupplier) {
			streamSupplier.getEndOfStream()
					.whenResult(acknowledgement::trySet)
					.whenException(this::closeEx);
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	static final class Skip<T> implements StreamConsumer<T> {
		private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		@Override
		public void consume(@NotNull StreamSupplier<T> streamSupplier) {
			streamSupplier.getEndOfStream()
					.whenResult(acknowledgement::trySet)
					.whenException(this::closeEx);
			if (getAcknowledgement().isComplete()) return;
			streamSupplier.resume(item -> {});
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			acknowledgement.trySetException(e);
		}
	}

	static final class OfPromise<T> implements StreamConsumer<T> {
		@Nullable StreamConsumer<T> actualConsumer;
		@Nullable StreamSupplier<T> supplier;
		final SettablePromise<Void> acknowledgement = new SettablePromise<>();

		public OfPromise(Promise<? extends StreamConsumer<T>> promise) {
			promise
					.whenResult(consumer -> {
						consumer.getAcknowledgement().whenComplete(this.acknowledgement::trySet);
						actualConsumer = consumer;
						if (actualConsumer.getAcknowledgement().isComplete()) {
							supplier = null;
							return;
						}
						if (acknowledgement.getException() != null) {
							actualConsumer.closeEx(acknowledgement.getException());
						} else if (supplier != null) {
							StreamSupplier<T> supplier = this.supplier;
							this.supplier = null;
							actualConsumer.consume(supplier);
						}
					})
					.whenException(this::closeEx);
		}

		@Override
		public void consume(@NotNull StreamSupplier<T> supplier) {
			supplier.getEndOfStream()
					.whenException(this::closeEx);
			if (getAcknowledgement().isComplete()) return;
			if (actualConsumer != null) {
				actualConsumer.consume(supplier);
			} else {
				this.supplier = supplier;
			}
		}

		@Override
		public Promise<Void> getAcknowledgement() {
			return acknowledgement;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			acknowledgement.trySetException(e);
			if (actualConsumer != null) {
				actualConsumer.closeEx(e);
			}
		}
	}

	static final class OfChannelConsumer<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private final ChannelQueue<T> queue;
		private final ChannelConsumer<T> consumer;

		OfChannelConsumer(ChannelConsumer<T> consumer) {
			this(new ChannelZeroBuffer<>(), consumer);
		}

		OfChannelConsumer(ChannelQueue<T> queue, ChannelConsumer<T> consumer) {
			this.queue = queue;
			this.consumer = consumer;
			queue.getSupplier().streamTo(consumer)
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onStarted() {
			resume(this);
		}

		@Override
		protected void onEndOfStream() {
			if (!queue.isWaitingPut()) {
				queue.put(null);
			}
		}

		@Override
		public void accept(T item) {
			assert item != null;
			Promise<Void> promise = queue.put(item);
			if (!promise.isComplete()) {
				suspend();
				promise.whenResult(() -> { // *
					if (isClosed()) return;
					if (!isEndOfStream()) {
						this.resume(this);
					} else {
						onEndOfStream();
					}
				});
			}
		}

		@Override
		protected void onError(Throwable e) {
			consumer.closeEx(e);
		}
	}

	static final class AsChannelConsumer<T> extends AbstractChannelConsumer<T> {
		private final StreamConsumer<T> streamConsumer;
		private StreamDataAcceptor<T> dataAcceptor;
		private T item;
		private SettablePromise<Void> itemPromise;
		final SettablePromise<Void> endOfStream = new SettablePromise<>();

		AsChannelConsumer(StreamConsumer<T> consumer) {
			streamConsumer = consumer;
			streamConsumer.getAcknowledgement()
					.whenResult(this::close)
					.whenException(this::closeEx);
			if (!streamConsumer.getAcknowledgement().isComplete()) {
				streamConsumer.consume(new StreamSupplier<T>() {
					@Override
					public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
						// *
						checkState(!isClosed());
						checkState(!streamConsumer.getAcknowledgement().isComplete());
						AsChannelConsumer.this.dataAcceptor = dataAcceptor;
						if (dataAcceptor != null) {
							if (item != null) {
								dataAcceptor.accept(item);
								itemPromise.set(null); // *
							}
						}
					}

					@Override
					public Promise<Void> getEndOfStream() {
						return endOfStream;
					}

					@Override
					public void closeEx(@NotNull Throwable e) {
						throw new AssertionError();
					}
				});
			}
		}

		@Override
		protected Promise<Void> doAccept(@Nullable T item) {
			assert !isClosed();
			assert !streamConsumer.getAcknowledgement().isComplete();
			if (item == null) {
				endOfStream.trySet(null);
				return streamConsumer.getAcknowledgement();
			}
			if (dataAcceptor != null) {
				dataAcceptor.accept(item);
				return Promise.complete();
			} else {
				this.item = item;
				this.itemPromise = new SettablePromise<>();
				this.itemPromise.whenComplete(() -> { // *
					this.item = null;
					this.itemPromise = null;
				});
				return this.itemPromise;
			}
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			dataAcceptor = null;
			endOfStream.trySetException(e);
			streamConsumer.closeEx(e);
			if (itemPromise != null) {
				itemPromise.setException(e); // *
			}
			checkState(this.dataAcceptor == null);
			checkState(this.item == null);
		}
	}

}
