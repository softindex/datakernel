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

import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;

import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;

public final class StreamConsumerSwitcher<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private InternalProducer currentInternalProducer;

	private StreamConsumerSwitcher(Eventloop eventloop) {
		super(eventloop);
	}

	public static <T> StreamConsumerSwitcher<T> create(Eventloop eventloop) {
		return create(eventloop, StreamConsumers.idle());
	}

	public static <T> StreamConsumerSwitcher<T> create(Eventloop eventloop, StreamConsumer<T> consumer) {
		StreamConsumerSwitcher<T> switcher = new StreamConsumerSwitcher<>(eventloop);
		switcher.switchTo(consumer);
		return switcher;
	}

	@Override
	public final void onData(T item) {
		currentInternalProducer.onData(item);
	}

	@Override
	protected final void onEndOfStream() {
		switchTo(StreamConsumers.idle());
	}

	@Override
	protected final void onError(Exception e) {
		switchTo(StreamConsumers.idle());
	}

	public void switchTo(StreamConsumer<T> newConsumer) {
		if (getStatus() == CLOSED_WITH_ERROR) {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendError(getException());
			}
			currentInternalProducer = new InternalProducer(eventloop, StreamConsumers.idle());
			StreamProducers.<T>closingWithError(eventloop, getException()).streamTo(newConsumer);
		} else if (getStatus() == END_OF_STREAM) {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendEndOfStream();
			}
			currentInternalProducer = new InternalProducer(eventloop, StreamConsumers.idle());
			StreamProducers.<T>closing(eventloop).streamTo(newConsumer);
		} else {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendEndOfStream();
			}
			currentInternalProducer = new InternalProducer(eventloop, newConsumer);
			newConsumer.streamFrom(currentInternalProducer);
		}
	}

	private class InternalProducer implements StreamProducer<T> {
		private final Eventloop eventloop;
		private final StreamConsumer<T> consumer;
		private StreamDataReceiver<T> lastDataReceiver;
		private boolean suspended;
		private ArrayList<T> pendingItems;
		private boolean pendingEndOfStream;

		public InternalProducer(Eventloop eventloop, StreamConsumer<T> consumer) {
			this.eventloop = eventloop;
			this.consumer = consumer;
		}

		@Override
		public void streamTo(StreamConsumer<T> consumer) {
			assert consumer == this.consumer;
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			lastDataReceiver = dataReceiver;
			suspended = false;

			if (pendingItems != null) {
				eventloop.post(() -> {
					if (pendingItems.isEmpty()) return;

					for (T item : pendingItems) {
						lastDataReceiver.onData(item);
					}
					pendingItems = null;

					if (pendingEndOfStream) {
						consumer.endOfStream();
					}

					if (StreamConsumerSwitcher.this.currentInternalProducer == this) {
						if (!suspended) {
							StreamConsumerSwitcher.this.getProducer().produce(StreamConsumerSwitcher.this);
						} else {
							StreamConsumerSwitcher.this.getProducer().suspend();
						}
					}
				});
			} else {
				if (StreamConsumerSwitcher.this.currentInternalProducer == this) {
					StreamConsumerSwitcher.this.getProducer().produce(StreamConsumerSwitcher.this);
				}
			}
		}

		@Override
		public void suspend() {
			suspended = true;
			if (StreamConsumerSwitcher.this.currentInternalProducer == this) {
				StreamConsumerSwitcher.this.getProducer().suspend();
			}
		}

		@Override
		public void closeWithError(Exception e) {
			StreamConsumerSwitcher.this.closeWithError(e);
		}

		public void onData(T item) {
			if (lastDataReceiver != null) {
				lastDataReceiver.onData(item);
			} else {
				if (pendingItems == null) {
					pendingItems = new ArrayList<>();
					StreamConsumerSwitcher.this.getProducer().suspend();
				}
				pendingItems.add(item);
			}
		}

		public void sendError(Exception exception) {
			lastDataReceiver = item -> {};
			consumer.closeWithError(exception);
		}

		public void sendEndOfStream() {
			if (pendingItems == null) {
				consumer.endOfStream();
			} else {
				pendingEndOfStream = true;
			}
		}
	}
}