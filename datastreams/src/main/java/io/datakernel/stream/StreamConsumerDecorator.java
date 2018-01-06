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

import java.util.concurrent.CompletionStage;

import static io.datakernel.stream.DataStreams.bind;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 *
 * @param <T> item type
 */
public abstract class StreamConsumerDecorator<T> implements StreamConsumer<T> {
	private StreamProducer<T> producer;
	private final SettableStage<Void> endOfStream = SettableStage.create();

	private StreamConsumer<T> actualConsumer;
	private final SettableStage<Void> internalEndOfStream = SettableStage.create();

	public StreamConsumerDecorator() {
	}

	public StreamConsumerDecorator(StreamConsumer<T> consumer) {
		setActualConsumer(consumer);
	}

	public final void setActualConsumer(StreamConsumer<T> consumer) {
		checkState(this.actualConsumer == null, "Decorator is already wired");
		actualConsumer = consumer;
		bind(new StreamProducer<T>() {
			@Override
			public void setConsumer(StreamConsumer<T> consumer) {
				assert consumer == actualConsumer;
				consumer.getEndOfStream()
						.whenComplete(Stages.onResult(this::onEndOfStream))
						.whenComplete(Stages.onError(this::onCloseWithError));
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				dataReceiver = onProduce(dataReceiver);
				if (dataReceiver != null) {
					producer.produce(dataReceiver);
				}
			}

			@Override
			public void suspend() {
				onSuspend();
			}

			private void onEndOfStream() {
				internalEndOfStream.trySet(null);
				StreamConsumerDecorator.this.onEndOfStream();
			}

			private void onCloseWithError(Throwable t) {
				internalEndOfStream.trySetException(t);
				StreamConsumerDecorator.this.onCloseWithError(t);
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return internalEndOfStream;
			}

		}, actualConsumer);
	}

	public final StreamConsumer<T> getActualConsumer() {
		return actualConsumer;
	}

	@Override
	public final void setProducer(StreamProducer<T> producer) {
		checkNotNull(producer);
		checkState(this.producer == null);
		this.producer = producer;
		producer.getEndOfStream()
				.whenComplete(Stages.onResult(this::sendEndOfStream))
				.whenComplete(Stages.onError(this::closeWithError));
	}

	public final StreamProducer<T> getProducer() {
		return producer;
	}

	@Override
	public final CompletionStage<Void> getEndOfStream() {
		return endOfStream;
	}

	protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
		return dataReceiver;
	}

	protected void onSuspend() {
		producer.suspend();
	}

	protected final void sendEndOfStream() {
		internalEndOfStream.trySet(null);
		endOfStream.trySet(null);
	}

	protected final void closeWithError(Throwable t) {
		internalEndOfStream.trySetException(t);
		endOfStream.trySetException(t);
	}

	protected void onEndOfStream() {
		sendEndOfStream();
	}

	protected void onCloseWithError(Throwable t) {
		closeWithError(t);
	}

}