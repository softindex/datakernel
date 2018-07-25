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
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.Set;

import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Collections.emptySet;

public final class StreamConsumerSwitcher<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private InternalProducer currentInternalProducer;

	private StreamConsumerSwitcher() {
	}

	public static <T> StreamConsumerSwitcher<T> create() {
		return create(StreamConsumer.idle());
	}

	public static <T> StreamConsumerSwitcher<T> create(StreamConsumer<T> consumer) {
		StreamConsumerSwitcher<T> switcher = new StreamConsumerSwitcher<>();
		switcher.switchTo(consumer);
		return switcher;
	}

	@Override
	public final void onData(T item) {
		currentInternalProducer.onData(item);
	}

	@Override
	protected final void onEndOfStream() {
		switchTo(StreamConsumer.idle());
	}

	@Override
	protected final void onError(Throwable t) {
		switchTo(StreamConsumer.idle());
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return currentInternalProducer == null ? emptySet() : currentInternalProducer.consumer.getCapabilities();
	}

	public void switchTo(StreamConsumer<T> newConsumer) {
		if (getStatus() == CLOSED_WITH_ERROR) {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendError(getException());
			}
			currentInternalProducer = new InternalProducer(eventloop, StreamConsumer.idle());
			StreamProducer.<T>closingWithError(getException()).streamTo(newConsumer);
		} else if (getStatus() == END_OF_STREAM) {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendEndOfStream();
			}
			currentInternalProducer = new InternalProducer(eventloop, StreamConsumer.idle());
			StreamProducer.<T>of().streamTo(newConsumer);
		} else {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendEndOfStream();
			}
			currentInternalProducer = new InternalProducer(eventloop, newConsumer);
			currentInternalProducer.streamTo(newConsumer);
		}
	}

	private class InternalProducer implements StreamProducer<T> {
		private final Eventloop eventloop;
		private final StreamConsumer<T> consumer;
		private final SettableStage<Void> endOfStream = new SettableStage<>();
		private StreamDataReceiver<T> lastDataReceiver;
		private boolean suspended;
		private ArrayList<T> pendingItems;
		private boolean pendingEndOfStream;

		public InternalProducer(Eventloop eventloop, StreamConsumer<T> consumer) {
			this.eventloop = eventloop;
			this.consumer = consumer;
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			assert consumer == this.consumer;
			consumer.getEndOfStream()
//					.thenRun(this::onEndOfStream)
					.whenException(this::closeWithError);
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			lastDataReceiver = dataReceiver;
			suspended = false;

			if (pendingItems != null) {
				eventloop.post(() -> {
					if (pendingItems.isEmpty()) {
						return;
					}

					for (T item : pendingItems) {
						lastDataReceiver.onData(item);
					}
					pendingItems = null;

					if (pendingEndOfStream) {
						endOfStream.trySet(null);
					}

					if (currentInternalProducer == this) {
						if (!suspended) {
							getProducer().produce(StreamConsumerSwitcher.this);
						} else {
							getProducer().suspend();
						}
					}
				});
			} else {
				if (currentInternalProducer == this) {
					StreamProducer<T> producer = getProducer();
					if (producer != null) {
						producer.produce(StreamConsumerSwitcher.this);
					}
				}
			}
		}

		@Override
		public void suspend() {
			suspended = true;
			if (currentInternalProducer == this) {
				getProducer().suspend();
			}
		}

		public void closeWithError(Throwable t) {
			StreamConsumerSwitcher.this.closeWithError(t);
		}

		@Override
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return getProducer().getCapabilities();
		}

		public void onData(T item) {
			if (lastDataReceiver != null) {
				lastDataReceiver.onData(item);
			} else {
				if (pendingItems == null) {
					pendingItems = new ArrayList<>();
					getProducer().suspend();
				}
				pendingItems.add(item);
			}
		}

		public void sendError(Throwable exception) {
			lastDataReceiver = item -> {};
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
