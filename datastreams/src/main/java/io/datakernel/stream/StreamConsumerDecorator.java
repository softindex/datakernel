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
public abstract class StreamConsumerDecorator<T, X> implements StreamConsumerWithResult<T, X>, HasEndOfStream {
	private final SettableStage<Void> endOfStream = SettableStage.create();
	private final SettableStage<X> result = SettableStage.create();
	private StreamProducer<T> producer;
	private StreamConsumer<T> actualConsumer;

	private Throwable pendingException;
	private boolean pendingEndOfStream;

	public final void setActualConsumer(StreamConsumer<T> consumer, CompletionStage<X> consumerResult) {
		setActualConsumer(consumer);
		setResult(consumerResult);
	}

	public final void setActualConsumer(StreamConsumer<T> consumer) {
		checkState(this.actualConsumer == null, "Decorator is already wired");
		actualConsumer = consumer;
		actualConsumer.setProducer(new StreamProducer<T>() {
			@Override
			public void setConsumer(StreamConsumer<T> consumer) {
				assert consumer == actualConsumer;
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				dataReceiver = onProduce(dataReceiver);
				producer.produce(dataReceiver);
			}

			@Override
			public void suspend() {
				onSuspend();
			}

			@Override
			public void closeWithError(Throwable t) {
				producer.closeWithError(t);
			}
		});
		if (pendingException != null) {
			actualConsumer.closeWithError(pendingException);
			endOfStream.trySetException(pendingException);
			result.trySetException(pendingException);
		} else if (pendingEndOfStream) {
			actualConsumer.endOfStream();
			endOfStream.trySet(null);
		}
	}

	public final void setResult(CompletionStage<X> result) {
		result.whenCompleteAsync((x, throwable) -> {
			if (throwable == null) {
				this.result.trySet(x);
			} else {
				producer.closeWithError(throwable);
				this.result.trySetException(throwable);
			}
		});
	}

	public final StreamProducer<T> getProducer() {
		return producer;
	}

	public final StreamConsumer<T> getActualConsumer() {
		return actualConsumer;
	}

	@Override
	public final void setProducer(StreamProducer<T> producer) {
		checkNotNull(producer);
		checkState(this.producer == null);
		this.producer = producer;
	}

	@Override
	public final void endOfStream() {
		if (actualConsumer != null) {
			actualConsumer.endOfStream();
			endOfStream.trySet(null);
		} else {
			pendingEndOfStream = true;
		}
	}

	@Override
	public final void closeWithError(Throwable t) {
		if (actualConsumer != null) {
			actualConsumer.closeWithError(t);
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

	protected void onSuspend() {
		producer.suspend();
	}

}
