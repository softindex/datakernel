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

import static com.google.common.base.Preconditions.checkState;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public abstract class StreamConsumerDecorator<T> implements StreamConsumer<T> {

	private StreamConsumer<T> actualConsumer;

	// region creators
	public StreamConsumerDecorator() {

	}

	public StreamConsumerDecorator(StreamConsumer<T> actualConsumer) {
		setActualConsumer(actualConsumer);
	}
	// region creators

	public void setActualConsumer(StreamConsumer<T> actualConsumer) {
		checkState(this.actualConsumer == null, "Decorator is already wired");
		this.actualConsumer = actualConsumer;
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return actualConsumer.getDataReceiver();
	}

	@Override
	public final void streamFrom(StreamProducer<T> upstreamProducer) {
		actualConsumer.streamFrom(upstreamProducer);
	}

	@Override
	public void onProducerEndOfStream() {
		actualConsumer.onProducerEndOfStream();
	}

	@Override
	public void onProducerError(Exception e) {
		actualConsumer.onProducerError(e);
	}

	@Override
	public final StreamStatus getConsumerStatus() {
		return actualConsumer.getConsumerStatus();
	}

	@Override
	public Exception getConsumerException() {
		return actualConsumer.getConsumerException();
	}
}
