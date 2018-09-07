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

import io.datakernel.async.Cancellable;
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.serial.AbstractSerialConsumer;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.stream.processor.StreamLateBinder;
import io.datakernel.stream.processor.StreamTransformer;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkArgument;

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

	MaterializedStage<Void> getAcknowledgement();

	Set<StreamCapability> getCapabilities();

	static <T> StreamConsumer<T> idle() {
		return new StreamConsumers.Idle<>();
	}

	static <T> StreamConsumer<T> skip() {
		return new StreamConsumers.Skip<>();
	}

	static <T> StreamConsumer<T> closingWithError(Throwable exception) {
		return new StreamConsumers.ClosingWithErrorImpl<>(exception);
	}

	static <T> StreamConsumer<T> ofSerialConsumer(SerialConsumer<T> consumer) {
		return new StreamConsumers.OfSerialConsumerImpl<>(consumer);
	}

	static <T> StreamConsumer<T> ofProducer(Consumer<StreamProducer<T>> producerAcceptor) {
		StreamTransformer<T, T> forwarder = StreamTransformer.identity();
		producerAcceptor.accept(forwarder.getOutput());
		return forwarder.getInput();
	}

	default <R> StreamConsumer<R> apply(StreamConsumerModifier<T, R> modifier) {
		return apply((Function<StreamConsumer<T>, StreamConsumer<R>>) modifier::applyTo);
	}

	default <R> R apply(Function<StreamConsumer<T>, R> fn) {
		return fn.apply(this);
	}

	default StreamConsumer<T> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : apply(StreamLateBinder.create());
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

	default StreamConsumer<T> whenEndOfStream(Runnable runnable) {
		getAcknowledgement().whenResult($ -> runnable.run());
		return this;
	}

	default StreamConsumer<T> whenException(Consumer<Throwable> consumer) {
		getAcknowledgement().whenException(consumer);
		return this;
	}

}
