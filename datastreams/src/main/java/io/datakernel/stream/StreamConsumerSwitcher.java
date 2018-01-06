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
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

import static io.datakernel.stream.DataStreams.bind;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;

public final class StreamConsumerSwitcher<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private InternalProducer currentInternalProducer;

	private StreamConsumerSwitcher() {
	}

	public static <T> StreamConsumerSwitcher<T> create() {
		return create(StreamConsumers.idle());
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
		switchTo(StreamConsumers.idle());
	}

	@Override
	protected final void onError(Throwable t) {
		switchTo(StreamConsumers.idle());
	}

	public void switchTo(StreamConsumer<T> newConsumer) {
		if (getStatus() == CLOSED_WITH_ERROR) {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendError(getException());
			}
			currentInternalProducer = new InternalProducer(eventloop, StreamConsumers.idle());
			bind(StreamProducers.closingWithError(getException()), newConsumer);
		} else if (getStatus() == END_OF_STREAM) {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendEndOfStream();
			}
			currentInternalProducer = new InternalProducer(eventloop, StreamConsumers.idle());
			bind(StreamProducers.closing(), newConsumer);
		} else {
			if (currentInternalProducer != null) {
				currentInternalProducer.sendEndOfStream();
			}
			currentInternalProducer = new InternalProducer(eventloop, newConsumer);
			bind(currentInternalProducer, newConsumer);
		}
	}

	private class InternalProducer implements StreamProducer<T> {
		private final Eventloop eventloop;
		private final StreamConsumer<T> consumer;
		private final SettableStage<Void> endOfStream = SettableStage.create();
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
//					.whenComplete(Stages.onResult(this::onEndOfStream))
					.whenComplete(Stages.onError(this::closeWithError));
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
						endOfStream.trySet(null);
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

		public void closeWithError(Throwable t) {
			StreamConsumerSwitcher.this.closeWithError(t);
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
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