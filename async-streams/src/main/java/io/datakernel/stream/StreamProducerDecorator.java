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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public abstract class StreamProducerDecorator<T> implements StreamProducer<T> {
	protected final Eventloop eventloop;

	protected StreamProducer<T> actualProducer;
	protected StreamConsumer<T> actualConsumer;

	public StreamProducerDecorator(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public StreamProducerDecorator(Eventloop eventloop, StreamProducer<T> actualProducer) {
		this.eventloop = eventloop;
		setActualProducer(actualProducer);
	}

	public final void setActualProducer(StreamProducer<T> actualProducer) {
		this.actualProducer = checkNotNull(actualProducer);
	}

	@Override
	public final void streamTo(final StreamConsumer<T> actualConsumer) {
		checkNotNull(actualConsumer);
		boolean firstTime = this.actualConsumer == null;
		this.actualConsumer = actualConsumer;
		this.actualProducer.streamTo(new StreamConsumer<T>() {
			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return actualConsumer.getDataReceiver();
			}

			@Override
			public void streamFrom(StreamProducer<T> upstreamProducer) {
				actualConsumer.streamFrom(upstreamProducer);
			}

			@Override
			public void onProducerEndOfStream() {
				StreamProducerDecorator.this.onProducerEndOfStream();
			}

			@Override
			public void onProducerError(Exception e) {
				StreamProducerDecorator.this.onProducerError(e);
			}

			@Override
			public void addConsumerCompletionCallback(CompletionCallback completionCallback) {
				throw new UnsupportedOperationException(); // not needed, it will not be called from outside
			}
		});

		if (firstTime) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					StreamProducerDecorator.this.onProducerStarted();
				}
			});
		}
	}

	@Override
	public final void bindDataReceiver() {
		actualProducer.bindDataReceiver();
	}

	@Override
	public final void addProducerCompletionCallback(CompletionCallback completionCallback) {
		actualProducer.addProducerCompletionCallback(completionCallback);
	}

	// extension hooks, intended for override:

	protected void onProducerStarted() {
	}

	@Override
	public void onConsumerSuspended() {
		actualProducer.onConsumerSuspended();
	}

	@Override
	public void onConsumerResumed() {
		actualProducer.onConsumerResumed();
	}

	@Override
	public void onConsumerError(Exception e) {
		actualProducer.onConsumerError(e);
	}

	private void onProducerError(Exception e) {
		actualConsumer.onProducerError(e);
	}

	protected void onProducerEndOfStream() {
		actualConsumer.onProducerEndOfStream();
	}

}
