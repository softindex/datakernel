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

import io.datakernel.async.*;
import io.datakernel.serial.AbstractSerialConsumer;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.stream.processor.StreamLateBinder;
import io.datakernel.stream.processor.StreamSkip;
import io.datakernel.stream.processor.StreamSkip.Dropper;
import io.datakernel.stream.processor.StreamSkip.SizeCounter;
import io.datakernel.stream.processor.StreamSkip.SkipStrategy;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Collections.emptySet;

/**
 * It represents an object which can asynchronous receive streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 */
public interface StreamConsumer<T> extends Cancellable {
	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param producer stream producer for setting
	 */
	void setProducer(StreamProducer<T> producer);

	MaterializedStage<Void> getEndOfStream();

	Set<StreamCapability> getCapabilities();

	default <R> StreamConsumer<R> with(StreamConsumerModifier<T, R> modifier) {
		StreamConsumer<T> consumer = this;
		return modifier.applyTo(consumer);
	}

	default StreamConsumer<T> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : with(StreamLateBinder.create());
	}

	static <T> StreamConsumer<T> idle() {
		return new StreamConsumers.IdleImpl<>();
	}

	static <T> StreamConsumer<T> closingWithError(Throwable exception) {
		return new StreamConsumers.ClosingWithErrorImpl<>(exception);
	}

	static <T> StreamConsumer<T> ofSerialConsumer(SerialConsumer<T> consumer) {
		return new StreamConsumers.OfSerialConsumerImpl<>(consumer);
	}

	default SerialConsumer<T> asSerialConsumer() {
		StreamProducerEndpoint<T> endpoint = new StreamProducerEndpoint<>();
		endpoint.streamTo(this);
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return endpoint.put(value);
			}
		};
	}

	String LATE_BINDING_ERROR_MESSAGE = "" +
			"StreamConsumer %s does not have LATE_BINDING capabilities, " +
			"it must be bound in the same tick when it is created. " +
			"Alternatively, use .withLateBinding() modifier";

	static <T> StreamConsumer<T> ofStage(Stage<? extends StreamConsumer<T>> consumerStage) {
		StreamLateBinder<T> lateBounder = StreamLateBinder.create();
		consumerStage.whenComplete((consumer, throwable) -> {
			if (throwable == null) {
				checkArgument(consumer.getCapabilities().contains(LATE_BINDING),
						LATE_BINDING_ERROR_MESSAGE, consumer);
				lateBounder.getOutput().streamTo(consumer);
			} else {
				lateBounder.getOutput().streamTo(closingWithError(throwable));
			}
		});
		return lateBounder.getInput();
	}

	default <X> StreamConsumerWithResult<T, X> withResultAsyncSupplier(AsyncSupplier<X> result) {
		return withResult(this.getEndOfStream().thenCompose($ -> result.get()));
	}

	default <X> StreamConsumerWithResult<T, X> withResultSupplier(Supplier<X> result) {
		return withResult(this.getEndOfStream().thenCompose($ -> Stage.of(result.get())));
	}

	default <X> StreamConsumerWithResult<T, X> withResult(Stage<X> result) {
		MaterializedStage<Void> endOfStream = getEndOfStream();
		MaterializedStage<X> safeResult = result.combine(endOfStream, (x, $) -> x).materialize();
		return new StreamConsumerWithResult<T, X>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				StreamConsumer.this.setProducer(producer);
			}

			@Override
			public MaterializedStage<Void> getEndOfStream() {
				return endOfStream;
			}

			@Override
			public MaterializedStage<X> getResult() {
				return safeResult;
			}

			@Override
			public Set<StreamCapability> getCapabilities() {
				return StreamConsumer.this.getCapabilities().contains(LATE_BINDING) ?
						EnumSet.of(LATE_BINDING) : emptySet();
			}

			@Override
			public void closeWithError(Throwable e) {
				StreamConsumer.this.closeWithError(e);
			}
		};
	}

	default StreamConsumerWithResult<T, Void> withEndOfStreamAsResult() {
		MaterializedStage<Void> endOfStream = getEndOfStream();
		return new StreamConsumerWithResult<T, Void>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				StreamConsumer.this.setProducer(producer);
			}

			@Override
			public MaterializedStage<Void> getEndOfStream() {
				return endOfStream;
			}

			@Override
			public MaterializedStage<Void> getResult() {
				return endOfStream;
			}

			@Override
			public Set<StreamCapability> getCapabilities() {
				return StreamConsumer.this.getCapabilities().contains(LATE_BINDING) ?
						EnumSet.of(LATE_BINDING) : emptySet();
			}

			@Override
			public void closeWithError(Throwable e) {
				StreamConsumer.this.closeWithError(e);
			}
		};
	}

	default StreamConsumer<T> whenEndOfStream(Runnable runnable) {
		getEndOfStream().whenResult($ -> runnable.run());
		return this;
	}

	default StreamConsumer<T> whenException(Consumer<Throwable> consumer) {
		getEndOfStream().whenException(consumer);
		return this;
	}

	default StreamConsumer<T> ignoreFirst(long skip, SizeCounter<T> sizeCounter, Dropper<T> dropper) {
		return with(StreamSkip.create(skip, sizeCounter, dropper));
	}

	default StreamConsumer<T> ignoreFirst(long skip, SkipStrategy<T> strategy) {
		return with(StreamSkip.create(skip, strategy));
	}

	default StreamConsumer<T> ignoreFirst(long skip) {
		return with(StreamSkip.create(skip));
	}

}
