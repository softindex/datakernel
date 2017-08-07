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
	protected StreamProducer<T> delegateProducer;

	// region creators
	protected StreamProducerDecorator() {
	}

	protected StreamProducerDecorator(StreamProducer<T> delegateProducer) {
		setDelegateProducer(delegateProducer);
	}
	// endregion

	protected void setDelegateProducer(StreamProducer<T> delegateProducer) {
		checkState(this.delegateProducer == null, "Decorator is already wired");
		this.delegateProducer = delegateProducer;
	}

	@Override
	public void streamTo(StreamConsumer<T> downstreamConsumer) {
		delegateProducer.streamTo(downstreamConsumer);
	}

	@Override
	public void bindDataReceiver() {
		delegateProducer.bindDataReceiver();
	}

	@Override
	public void onConsumerSuspended() {
		delegateProducer.onConsumerSuspended();
	}

	@Override
	public void onConsumerResumed() {
		delegateProducer.onConsumerResumed();
	}

	@Override
	public void onConsumerError(Exception e) {
		delegateProducer.onConsumerError(e);
	}

	@Override
	public StreamStatus getProducerStatus() {
		return delegateProducer.getProducerStatus();
	}

	@Override
	public Exception getProducerException() {
		return delegateProducer.getProducerException();
	}
}