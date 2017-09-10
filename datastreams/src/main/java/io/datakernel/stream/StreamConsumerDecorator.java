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

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public class StreamConsumerDecorator<T> implements StreamConsumer<T> {
	private StreamProducer<T> producer;
	private StreamConsumer<T> actualConsumer;

	private Exception pendingError;
	private boolean pendingEndOfStream;

	// region creators
	protected StreamConsumerDecorator() {
	}

	protected StreamConsumerDecorator(StreamConsumer<T> actualConsumer) {
		setActualConsumer(actualConsumer);
	}

	public static <T> StreamConsumerDecorator<T> create() {
		return new StreamConsumerDecorator<T>();
	}
	// region creators

	public final void setActualConsumer(StreamConsumer<T> consumer) {
		checkState(this.actualConsumer == null, "Decorator is already wired");
		actualConsumer = consumer;
		actualConsumer.streamFrom(new StreamProducer<T>() {
			@Override
			public void streamTo(StreamConsumer<T> consumer) {
				assert consumer == actualConsumer;
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				onProduce(dataReceiver);
			}

			@Override
			public void suspend() {
				onSuspend();
			}

			@Override
			public void closeWithError(Exception e) {
				producer.closeWithError(e);
			}
		});
		if (pendingError != null) {
			actualConsumer.closeWithError(pendingError);
		} else if (pendingEndOfStream) {
			actualConsumer.endOfStream();
		}
	}

	public final StreamConsumer<T> getActualConsumer() {
		return actualConsumer;
	}

	@Override
	public final void streamFrom(StreamProducer<T> producer) {
		checkNotNull(producer);
		if (this.producer == producer) return;

		checkState(this.producer == null);

		this.producer = producer;
		producer.streamTo(this);
	}

	@Override
	public final void endOfStream() {
		if (actualConsumer != null) {
			actualConsumer.endOfStream();
		} else {
			pendingEndOfStream = true;
		}
	}

	@Override
	public final void closeWithError(Exception e) {
		if (actualConsumer != null) {
			actualConsumer.closeWithError(e);
		} else {
			pendingError = e;
		}
	}

	protected void onProduce(StreamDataReceiver<T> dataReceiver) {
		producer.produce(onReceiver(dataReceiver));
	}

	protected StreamDataReceiver<T> onReceiver(StreamDataReceiver<T> dataReceiver) {
		return dataReceiver;
	}

	protected void onSuspend() {
		producer.suspend();
	}

}
