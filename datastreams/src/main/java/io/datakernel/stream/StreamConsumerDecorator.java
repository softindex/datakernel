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
public abstract class StreamConsumerDecorator<T, X> implements StreamConsumerWithResult<T, X> {
	private final SettableStage<Void> completionStage = SettableStage.create();
	private final SettableStage<X> resultStage = SettableStage.create();
	private StreamProducer<T> producer;
	private StreamConsumer<T> actualConsumer;

	private Throwable pendingException;
	private boolean pendingEndOfStream;

	public final void setActualConsumer(StreamConsumer<T> consumer) {
		setActualConsumer(consumer, SettableStage.create());
	}

	public final void setActualConsumer(StreamConsumer<T> consumer, CompletionStage<X> consumerResult) {
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
			completionStage.trySetException(pendingException);
			resultStage.trySetException(pendingException);
		} else if (pendingEndOfStream) {
			actualConsumer.endOfStream();
			completionStage.trySet(null);
		}
		consumerResult.whenCompleteAsync((x, throwable) -> {
			if (throwable == null) {
				resultStage.trySet(x);
			} else {
				producer.closeWithError(throwable);
				resultStage.trySetException(throwable);
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
			completionStage.trySet(null);
		} else {
			pendingEndOfStream = true;
		}
	}

	@Override
	public final void closeWithError(Throwable t) {
		if (actualConsumer != null) {
			actualConsumer.closeWithError(t);
			completionStage.trySetException(t);
			resultStage.trySetException(t);
		} else {
			pendingException = t;
		}
	}

	@Override
	public final CompletionStage<X> getResult() {
		return resultStage;
	}

	public final CompletionStage<Void> getCompletion() {
		return completionStage;
	}

	protected StreamDataReceiver<T> onProduce(StreamDataReceiver<T> dataReceiver) {
		return dataReceiver;
	}

	protected void onSuspend() {
		producer.suspend();
	}

}
