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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.stream.StreamStatus.SUSPENDED;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public class StreamProducerDecorator<T> implements StreamProducer<T> {
	private StreamProducer<T> actualProducer;
	private StreamConsumer<T> consumer;

	private StreamDataReceiver<T> pendingDataReceiver;
	private Exception pendingError;

	// region creators
	protected StreamProducerDecorator() {
	}

	protected StreamProducerDecorator(StreamProducer<T> actualProducer) {
		setActualProducer(actualProducer);
	}

	public static <T> StreamProducerDecorator<T> create() {
		return new StreamProducerDecorator<T>();
	}
	// endregion

	public final void setActualProducer(StreamProducer<T> producer) {
		checkState(this.actualProducer == null, "Decorator is already wired");
		actualProducer = producer;
		actualProducer.streamTo(new StreamConsumer<T>() {
			@Override
			public void streamFrom(StreamProducer<T> producer) {
				assert producer == actualProducer;
			}

			@Override
			public void endOfStream() {
				onEndOfStream();
			}

			@Override
			public void closeWithError(Exception e) {
				onCloseWithError(e);
			}
		});
		if (pendingError != null) {
			actualProducer.closeWithError(pendingError);
		} else if (pendingDataReceiver != null) {
			actualProducer.produce(pendingDataReceiver);
		}
	}

	public final StreamProducer<T> getActualProducer() {
		return actualProducer;
	}

	public StreamConsumer<T> getConsumer() {
		return consumer;
	}

	@Override
	public final void streamTo(StreamConsumer<T> consumer) {
		checkNotNull(consumer);
		if (this.consumer == consumer) return;
		checkState(this.consumer == null);

		this.consumer = consumer;

		consumer.streamFrom(this);
	}

	@Override
	public final void produce(StreamDataReceiver<T> dataReceiver) {
		if (actualProducer != null) {
			actualProducer.produce(dataReceiver);
		} else {
			pendingDataReceiver = dataReceiver;
		}
	}

	@Override
	public final void suspend() {
		if (actualProducer != null) {
			actualProducer.suspend();
		} else {
			pendingDataReceiver = null;
		}
	}

	@Override
	public final void closeWithError(Exception e) {
		if (actualProducer != null) {
			actualProducer.closeWithError(e);
		} else {
			pendingError = e;
		}
	}

	protected void onEndOfStream() {
		consumer.endOfStream();
	}

	protected void onCloseWithError(Exception e) {
		consumer.closeWithError(e);
	}

}