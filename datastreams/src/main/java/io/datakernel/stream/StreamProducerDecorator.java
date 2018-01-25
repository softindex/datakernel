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

import java.util.Set;
import java.util.concurrent.CompletionStage;

import static io.datakernel.stream.DataStreams.bind;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.emptySet;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 *
 * @param <T> item type
 */
public abstract class StreamProducerDecorator<T> implements StreamProducer<T> {
	private StreamConsumer<T> consumer;
	private final SettableStage<Void> endOfStream = SettableStage.create();

	private StreamProducer<T> actualProducer;
	private final SettableStage<Void> internalEndOfStream = SettableStage.create();

	private StreamDataReceiver<T> pendingDataReceiver;

	public StreamProducerDecorator() {
	}

	public StreamProducerDecorator(StreamProducer<T> producer) {
		setActualProducer(producer);
	}

	public final void setActualProducer(StreamProducer<T> producer) {
		checkState(this.actualProducer == null, "Decorator is already wired");
		actualProducer = producer;
		bind(actualProducer, new StreamConsumer<T>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				assert producer == actualProducer;
				producer.getEndOfStream()
						.whenComplete(Stages.onResult(this::onEndOfStream))
						.whenComplete(Stages.onError(this::onCloseWithError));
			}

			private void onEndOfStream() {
				internalEndOfStream.trySet(null);
				StreamProducerDecorator.this.onEndOfStream();
			}

			private void onCloseWithError(Throwable t) {
				internalEndOfStream.trySetException(t);
				StreamProducerDecorator.this.onCloseWithError(t);
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return internalEndOfStream;
			}

			@Override
			public Set<StreamCapability> getCapabilities() {
				throw new UnsupportedOperationException();
			}
		});
		if (pendingDataReceiver != null) {
			actualProducer.produce(pendingDataReceiver);
		}
	}

	public final StreamProducer<T> getActualProducer() {
		return actualProducer;
	}

	@Override
	public final void setConsumer(StreamConsumer<T> consumer) {
		checkNotNull(consumer);
		checkState(this.consumer == null);
		this.consumer = consumer;
		consumer.getEndOfStream()
				.whenComplete(Stages.onResult(this::sendEndOfStream))
				.whenComplete(Stages.onError(this::closeWithError));
	}

	public final StreamConsumer<T> getConsumer() {
		return consumer;
	}

	@Override
	public final void produce(StreamDataReceiver<T> dataReceiver) {
		dataReceiver = onProduce(dataReceiver);
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
	public final CompletionStage<Void> getEndOfStream() {
		return endOfStream;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return emptySet();
	}

	protected final void sendEndOfStream() {
		internalEndOfStream.trySet(null);
		endOfStream.trySet(null);
	}

	protected final void closeWithError(Throwable t) {
		internalEndOfStream.trySetException(t);
		endOfStream.trySetException(t);
	}

	protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
		return dataReceiver;
	}

	protected void onEndOfStream() {
		sendEndOfStream();
	}

	protected void onCloseWithError(Throwable t) {
		closeWithError(t);
	}

}