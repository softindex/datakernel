/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.serial.AbstractSerialConsumer;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.stream.StreamConsumers.ClosingWithErrorImpl;
import io.datakernel.stream.StreamConsumers.Idle;
import io.datakernel.stream.StreamConsumers.OfSerialConsumerImpl;
import io.datakernel.stream.StreamConsumers.Skip;
import io.datakernel.stream.processor.StreamLateBinder;
import io.datakernel.stream.processor.StreamTransformer;

import java.util.Set;
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
	 * Sets wired supplier. It will sent data to this consumer
	 *
	 * @param supplier stream supplier for setting
	 */
	void setSupplier(StreamSupplier<T> supplier);

	MaterializedPromise<Void> getAcknowledgement();

	Set<StreamCapability> getCapabilities();

	static <T> StreamConsumer<T> idle() {
		return new Idle<>();
	}

	static <T> StreamConsumer<T> skip() {
		return new Skip<>();
	}

	static <T> StreamConsumer<T> closingWithError(Throwable exception) {
		return new ClosingWithErrorImpl<>(exception);
	}

	static <T> StreamConsumer<T> ofSerialConsumer(SerialConsumer<T> consumer) {
		return new OfSerialConsumerImpl<>(consumer);
	}

	static <T> StreamConsumer<T> ofSupplier(Function<StreamSupplier<T>, MaterializedPromise<Void>> supplier) {
		StreamTransformer<T, T> forwarder = StreamTransformer.identity();
		MaterializedPromise<Void> extraAcknowledge = supplier.apply(forwarder.getOutput());
		StreamConsumer<T> result = forwarder.getInput();
		if (extraAcknowledge == Promise.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	default <R> R apply(StreamConsumerFunction<T, R> fn) {
		return fn.apply(this);
	}

	default StreamConsumer<T> withLateBinding() {
		return getCapabilities().contains(LATE_BINDING) ? this : apply(StreamLateBinder.create());
	}

	default SerialConsumer<T> asSerialConsumer() {
		StreamSupplierEndpoint<T> endpoint = new StreamSupplierEndpoint<>();
		endpoint.streamTo(this);
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T item) {
				if (item != null) return endpoint.put(item);
				assert endpoint.getConsumer() != null;
				return endpoint.put(null).both(endpoint.getConsumer().getAcknowledgement());
			}
		};
	}

	String LATE_BINDING_ERROR_MESSAGE = "" +
			"StreamConsumer %s does not have LATE_BINDING capabilities, " +
			"it must be bound in the same tick when it is created. " +
			"Alternatively, use .withLateBinding() modifier";

	static <T> StreamConsumer<T> ofPromise(Promise<? extends StreamConsumer<T>> promise) {
		if (promise.isResult()) return promise.materialize().getResult();
		StreamLateBinder<T> lateBounder = StreamLateBinder.create();
		promise.whenComplete((consumer, throwable) -> {
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

	default StreamConsumer<T> withAcknowledgement(Function<Promise<Void>, Promise<Void>> fn) {
		Promise<Void> acknowledgement = getAcknowledgement();
		Promise<Void> suppliedAcknowledgement = fn.apply(acknowledgement);
		if (acknowledgement == suppliedAcknowledgement) return this;
		MaterializedPromise<Void> newAcknowledgement = suppliedAcknowledgement.materialize();
		return new ForwardingStreamConsumer<T>(this) {
			@Override
			public MaterializedPromise<Void> getAcknowledgement() {
				return newAcknowledgement;
			}
		};
	}

}
