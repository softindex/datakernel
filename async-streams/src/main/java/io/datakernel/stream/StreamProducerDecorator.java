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
public abstract class StreamProducerDecorator<T> implements StreamProducer<T> {

	private StreamProducer<T> actualProducer;

	// region creators
	public StreamProducerDecorator() {

	}

	public StreamProducerDecorator(StreamProducer<T> actualProducer) {
		setActualProducer(actualProducer);
	}
	// endregion

	public void setActualProducer(StreamProducer<T> actualProducer) {
		checkState(this.actualProducer == null, "Decorator is already wired");
		this.actualProducer = actualProducer;
	}

	@Override
	public final void streamTo(StreamConsumer<T> downstreamConsumer) {
		actualProducer.streamTo(downstreamConsumer);
	}

	@Override
	public void bindDataReceiver() {
		actualProducer.bindDataReceiver();
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

	@Override
	public final StreamStatus getProducerStatus() {
		return actualProducer.getProducerStatus();
	}

	@Override
	public Exception getProducerException() {
		return actualProducer.getProducerException();
	}
}