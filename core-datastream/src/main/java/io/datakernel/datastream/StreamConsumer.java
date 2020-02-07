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

package io.datakernel.datastream;

import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.datastream.StreamConsumers.ClosingWithError;
import io.datakernel.datastream.StreamConsumers.Idle;
import io.datakernel.datastream.StreamConsumers.OfChannelConsumer;
import io.datakernel.datastream.StreamConsumers.Skip;
import io.datakernel.datastream.processor.StreamTransformer;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * It represents an object which can asynchronous receive streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 */
public interface StreamConsumer<T> extends AsyncCloseable {
	/**
	 * Sets wired supplier. It will sent data to this consumer
	 */
	void consume(@NotNull StreamSupplier<T> streamSupplier);

	Promise<Void> getAcknowledgement();

	static <T> StreamConsumer<T> idle() {
		return new Idle<>();
	}

	static <T> StreamConsumer<T> skip() {
		return new Skip<>();
	}

	/**
	 * @deprecated use of this consumer is discouraged as it breaks the whole asynchronous model.
	 * Exists only for testing
	 */
	@Deprecated
	static <T> StreamConsumer<T> of(Consumer<T> consumer) {
		return new StreamConsumers.OfConsumer<>(consumer);
	}

	static <T> StreamConsumer<T> closingWithError(Throwable e) {
		return new ClosingWithError<>(e);
	}

	static <T> StreamConsumer<T> ofChannelConsumer(ChannelConsumer<T> consumer) {
		return new OfChannelConsumer<>(consumer);
	}

	static <T> ChannelConsumer<T> asStreamConsumer(StreamConsumer<T> consumer) {
		return new StreamConsumers.AsChannelConsumer<>(consumer);
	}

	static <T> StreamConsumer<T> ofSupplier(Function<StreamSupplier<T>, Promise<Void>> supplier) {
		StreamTransformer<T, T> forwarder = StreamTransformer.identity();
		Promise<Void> extraAcknowledge = supplier.apply(forwarder.getOutput());
		StreamConsumer<T> result = forwarder.getInput();
		if (extraAcknowledge == Promise.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	default <R> R transformWith(StreamConsumerTransformer<T, R> fn) {
		return fn.transform(this);
	}

	static <T> StreamConsumer<T> ofPromise(Promise<? extends StreamConsumer<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new StreamConsumers.OfPromise<>(promise);

	}

	default StreamConsumer<T> withAcknowledgement(Function<Promise<Void>, Promise<Void>> fn) {
		Promise<Void> acknowledgement = getAcknowledgement();
		Promise<Void> newAcknowledgement = fn.apply(acknowledgement);
		if (acknowledgement == newAcknowledgement) return this;
		return new ForwardingStreamConsumer<T>(this) {
			@Override
			public Promise<Void> getAcknowledgement() {
				return newAcknowledgement;
			}
		};
	}

}
