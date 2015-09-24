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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public class StreamConsumerDecorator<T> implements StreamConsumer<T> {
	protected StreamConsumer<T> actualConsumer;
	protected StreamProducer<T> actualProducer;

	public StreamConsumerDecorator(final StreamConsumer<T> actualConsumer) {
		setActualConsumer(actualConsumer);
	}

	public final void setActualConsumer(final StreamConsumer<T> actualConsumer) {
		this.actualConsumer = checkNotNull(actualConsumer);
		this.actualConsumer.streamFrom(new StreamProducer<T>() {
			@Override
			public void streamTo(StreamConsumer<T> downstreamConsumer) {
				StreamConsumerDecorator.this.actualProducer.streamTo(actualConsumer);
			}

			@Override
			public void bindDataReceiver() {
				StreamConsumerDecorator.this.actualProducer.bindDataReceiver();
			}

			@Override
			public void onConsumerSuspended() {
				StreamConsumerDecorator.this.onConsumerSuspended();
			}

			@Override
			public void onConsumerResumed() {
				StreamConsumerDecorator.this.onConsumerResumed();
			}

			@Override
			public void onConsumerError(Exception e) {
				StreamConsumerDecorator.this.onConsumerError(e);
			}

			@Override
			public void addProducerCompletionCallback(CompletionCallback completionCallback) {
				throw new UnsupportedOperationException(); // not needed, it will not be called from outside
			}
		});
	}

	@Override
	public final StreamDataReceiver<T> getDataReceiver() {
		return actualConsumer.getDataReceiver();
	}

	@Override
	public final void streamFrom(StreamProducer<T> actualProducer) {
		this.actualProducer = actualProducer;
	}

	// extension hooks, intended for override:

	@Override
	public final void addConsumerCompletionCallback(CompletionCallback completionCallback) {
		actualConsumer.addConsumerCompletionCallback(completionCallback);
	}

	protected void onConsumerSuspended() {
		actualProducer.onConsumerSuspended();
	}

	protected void onConsumerResumed() {
		actualProducer.onConsumerResumed();
	}

	protected void onConsumerError(Exception e) {
		actualProducer.onConsumerError(e);
	}

	@Override
	public void onProducerEndOfStream() {
		actualConsumer.onProducerEndOfStream();
	}

	@Override
	public void onProducerError(Exception e) {
		actualConsumer.onProducerError(e);
	}

}
