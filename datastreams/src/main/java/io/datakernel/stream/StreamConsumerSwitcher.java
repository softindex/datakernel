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

package io.datakernel.stream;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.Set;

import static java.util.Collections.emptySet;

public final class StreamConsumerSwitcher<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
	private InternalSupplier currentInternalSupplier;
	private int pendingConsumers = 0;

	private StreamConsumerSwitcher() {
	}

	public static <T> StreamConsumerSwitcher<T> create() {
		return new StreamConsumerSwitcher<>();
	}

	public static <T> StreamConsumerSwitcher<T> create(StreamConsumer<T> consumer) {
		StreamConsumerSwitcher<T> switcher = new StreamConsumerSwitcher<>();
		switcher.switchTo(consumer);
		return switcher;
	}

	@Override
	public final void accept(T item) {
		currentInternalSupplier.onData(item);
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		if (currentInternalSupplier != null) {
			currentInternalSupplier.sendEndOfStream();
		}
		return getAcknowledgement();
	}

	@Override
	protected final void onError(Throwable t) {
		switchTo(StreamConsumer.idle());
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return currentInternalSupplier == null ? emptySet() : currentInternalSupplier.consumer.getCapabilities();
	}

	public void switchTo(StreamConsumer<T> newConsumer) {
		if (getSupplier() != null && getSupplier().getEndOfStream().isException()) {
			if (currentInternalSupplier != null) {
				currentInternalSupplier.sendError(getAcknowledgement().getException());
			}
			currentInternalSupplier = new InternalSupplier(eventloop, StreamConsumer.idle());
			StreamSupplier.<T>closingWithError(getAcknowledgement().getException()).streamTo(newConsumer);
		} else if (getSupplier() != null && getSupplier().getEndOfStream().isComplete()) {
			if (currentInternalSupplier != null) {
				currentInternalSupplier.sendEndOfStream();
			}
			currentInternalSupplier = new InternalSupplier(eventloop, StreamConsumer.idle());
			StreamSupplier.<T>of().streamTo(newConsumer);
		} else {
			if (currentInternalSupplier != null) {
				currentInternalSupplier.sendEndOfStream();
			}
			currentInternalSupplier = new InternalSupplier(eventloop, newConsumer);
			currentInternalSupplier.streamTo(newConsumer);
		}
	}

	private class InternalSupplier implements StreamSupplier<T> {
		private final Eventloop eventloop;
		private final StreamConsumer<T> consumer;
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();
		private StreamDataAcceptor<T> lastDataAcceptor;
		private boolean suspended;
		@Nullable
		private ArrayList<T> pendingItems;
		private boolean pendingEndOfStream;

		public InternalSupplier(Eventloop eventloop, StreamConsumer<T> consumer) {
			this.eventloop = eventloop;
			this.consumer = consumer;
			pendingConsumers++;
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			assert consumer == this.consumer;
			consumer.getAcknowledgement()
					.whenException(this::close)
					.post()
					.whenResult($ -> {
						if (--pendingConsumers == 0) {
							StreamConsumerSwitcher.this.acknowledge();
						}
					});
		}

		@Override
		public void resume(StreamDataAcceptor<T> dataAcceptor) {
			lastDataAcceptor = dataAcceptor;
			suspended = false;

			if (pendingItems != null) {
				eventloop.post(() -> {
					if (pendingItems.isEmpty()) {
						return;
					}

					for (T item : pendingItems) {
						lastDataAcceptor.accept(item);
					}
					pendingItems = null;

					if (pendingEndOfStream) {
						endOfStream.trySet(null);
					}

					if (currentInternalSupplier == this) {
						if (!suspended) {
							getSupplier().resume(StreamConsumerSwitcher.this);
						} else {
							getSupplier().suspend();
						}
					}
				});
			} else {
				if (currentInternalSupplier == this) {
					StreamSupplier<T> supplier = getSupplier();
					if (supplier != null) {
						supplier.resume(StreamConsumerSwitcher.this);
					}
				}
			}
		}

		@Override
		public void suspend() {
			suspended = true;
			if (currentInternalSupplier == this) {
				getSupplier().suspend();
			}
		}

		@Override
		public void close(Throwable t) {
			StreamConsumerSwitcher.this.close(t);
		}

		@Override
		public MaterializedPromise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return getSupplier().getCapabilities();
		}

		public void onData(T item) {
			if (lastDataAcceptor != null) {
				lastDataAcceptor.accept(item);
			} else {
				if (pendingItems == null) {
					pendingItems = new ArrayList<>();
					getSupplier().suspend();
				}
				pendingItems.add(item);
			}
		}

		public void sendError(Throwable exception) {
			lastDataAcceptor = item -> {};
			endOfStream.trySetException(exception);
		}

		public void sendEndOfStream() {
			if (pendingItems == null) {
				endOfStream.trySet(null);
			} else {
				pendingEndOfStream = true;
			}
		}
	}
}
