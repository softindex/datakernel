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

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.stream.processor.StreamLateBinder;

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
 *
 * @param <T> type of input data
 */
public interface StreamConsumer<T> {
	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param producer stream producer for setting
	 */
	void setProducer(StreamProducer<T> producer);

	Stage<Void> getEndOfStream();

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

	/**
	 * Creates a stream consumer which passes consumed items into a given lambda.
	 */
	static <T> StreamConsumer<T> ofConsumer(Consumer<T> consumer) {
		return new StreamConsumers.OfConsumerImpl<>(consumer);
	}

	static <T> StreamConsumer<T> ofAsyncConsumer(AsyncConsumer<T> consumer) {
		return new StreamConsumers.OfAsyncConsumerImpl<>(consumer);
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
		SettableStage<Void> safeEndOfStream = new SettableStage<>();
		SettableStage<X> safeResult = new SettableStage<>();
		getEndOfStream().whenComplete(($, throwable) -> {
			safeEndOfStream.trySet($, throwable);
			if (throwable != null) {
				safeResult.trySetException(throwable);
			}
		});
		result.post().whenComplete(safeResult::trySet);
		return new StreamConsumerWithResult<T, X>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				StreamConsumer.this.setProducer(producer);
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return safeEndOfStream;
			}

			@Override
			public Stage<X> getResult() {
				return safeResult;
			}

			@Override
			public Set<StreamCapability> getCapabilities() {
				return StreamConsumer.this.getCapabilities().contains(LATE_BINDING) ?
						EnumSet.of(LATE_BINDING) : emptySet();
			}
		};
	}

	default StreamConsumerWithResult<T, Void> withEndOfStreamAsResult() {
		SettableStage<Void> safeEndOfStream = new SettableStage<>();
		getEndOfStream().post().whenComplete(safeEndOfStream::trySet);
		return new StreamConsumerWithResult<T, Void>() {
			@Override
			public void setProducer(StreamProducer<T> producer) {
				StreamConsumer.this.setProducer(producer);
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return safeEndOfStream;
			}

			@Override
			public Stage<Void> getResult() {
				return safeEndOfStream;
			}

			@Override
			public Set<StreamCapability> getCapabilities() {
				return StreamConsumer.this.getCapabilities().contains(LATE_BINDING) ?
						EnumSet.of(LATE_BINDING) : emptySet();
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

}
