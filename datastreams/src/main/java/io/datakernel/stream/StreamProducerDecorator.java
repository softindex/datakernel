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

import java.util.concurrent.CompletionStage;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 *
 * @param <T> item type
 */
public abstract class StreamProducerDecorator<T, X> implements StreamProducerWithResult<T, X>, HasEndOfStream {
	private final SettableStage<Void> endOfStream = SettableStage.create();
	private final SettableStage<X> result = SettableStage.create();
	private StreamProducer<T> actualProducer;
	private StreamConsumer<T> consumer;

	private StreamDataReceiver<T> pendingDataReceiver;
	private Throwable pendingException;

	public final void setActualProducer(StreamProducer<T> producer, CompletionStage<X> producerResult) {
		setActualProducer(producer);
		setResult(producer, producerResult);
	}

	public final void setActualProducer(StreamProducer<T> producer) {
		checkState(this.actualProducer == null, "Decorator is already wired");
		actualProducer = producer;
		actualProducer.setConsumer(new StreamConsumer<T>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				assert producer == actualProducer;
			}

			@Override
			public void endOfStream() {
				onEndOfStream();
			}

			@Override
			public void closeWithError(Throwable t) {
				onCloseWithError(t);
			}
		});
		if (pendingException != null) {
			actualProducer.closeWithError(pendingException);
			endOfStream.trySetException(pendingException);
			result.trySetException(pendingException);
		} else if (pendingDataReceiver != null) {
			actualProducer.produce(pendingDataReceiver);
		}
	}

	public final void setResult(StreamProducer<T> producer, CompletionStage<X> producerResult) {
		producerResult.whenCompleteAsync((x, throwable) -> {
			if (throwable == null) {
				result.trySet(x);
			} else {
				producer.closeWithError(throwable);
				result.trySetException(throwable);
			}
		});
	}

	public final StreamProducer<T> getActualProducer() {
		return actualProducer;
	}

	public final StreamConsumer<T> getConsumer() {
		return consumer;
	}

	@Override
	public final void setConsumer(StreamConsumer<T> consumer) {
		checkNotNull(consumer);
		if (this.consumer == consumer) return;
		checkState(this.consumer == null);

		this.consumer = consumer;
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
	public final void closeWithError(Throwable t) {
		if (actualProducer != null) {
			actualProducer.closeWithError(t);
			endOfStream.trySetException(t);
			result.trySetException(t);
		} else {
			pendingException = t;
		}
	}

	@Override
	public final CompletionStage<Void> getEndOfStream() {
		return endOfStream;
	}

	@Override
	public final CompletionStage<X> getResult() {
		return result;
	}

	protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
		return dataReceiver;
	}

	protected void onEndOfStream() {
		consumer.endOfStream();
		endOfStream.trySet(null);
	}

	protected void onCloseWithError(Throwable t) {
		consumer.closeWithError(t);
		endOfStream.trySetException(t);
	}

}