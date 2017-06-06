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
	protected StreamConsumer<T> delegateConsumer;

	// region creators
	protected StreamConsumerDecorator() {
	}

	protected StreamConsumerDecorator(StreamConsumer<T> delegateConsumer) {
		setDelegateConsumer(delegateConsumer);
	}
	// region creators

	protected void setDelegateConsumer(StreamConsumer<T> delegateConsumer) {
		checkState(this.delegateConsumer == null, "Decorator is already wired");
		this.delegateConsumer = delegateConsumer;
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return delegateConsumer.getDataReceiver();
	}

	@Override
	public void streamFrom(StreamProducer<T> upstreamProducer) {
		delegateConsumer.streamFrom(upstreamProducer);
	}

	@Override
	public void onProducerEndOfStream() {
		delegateConsumer.onProducerEndOfStream();
	}

	@Override
	public void onProducerError(Exception e) {
		delegateConsumer.onProducerError(e);
	}

	@Override
	public StreamStatus getConsumerStatus() {
		return delegateConsumer.getConsumerStatus();
	}

	@Override
	public Exception getConsumerException() {
		return delegateConsumer.getConsumerException();
	}
}
