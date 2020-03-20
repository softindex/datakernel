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
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This interface represents an object that can asynchronously receive streams of data.
 * <p>
 * Implementors of this interface might want to extend {@link AbstractStreamConsumer}
 * instead of this interface, since it makes the threading and state management easier.
 */
public interface StreamConsumer<T> extends AsyncCloseable {
	/**
	 * Begins streaming data from the given supplier into this consumer.
	 * This method may not be called directly, use {@link StreamSupplier#streamTo} instead.
	 * <p>
	 * This method must have no effect after {@link #getAcknowledgement() the acknowledgement} is set.
	 */
	void consume(@NotNull StreamSupplier<T> streamSupplier);

	@Nullable
	StreamDataAcceptor<T> getDataAcceptor();

	/**
	 * A signal promise of the <i>acknowledgement</i> state of this consumer - its completion means that
	 * this consumer changed to that state and is now <b>closed</b>.
	 * <p>
	 * When the consumer is in this state nobody must send any more data to any of its related acceptors.
	 * <p>
	 * If promise completes with an error then this consumer closes with that error.
	 */
	Promise<Void> getAcknowledgement();

	/**
	 * Creates a consumer which does not consume anything.
	 * Its acknowledgement completes when the supplier closes.
	 */
	static <T> StreamConsumer<T> idle() {
		return new Idle<>();
	}

	/**
	 * Creates a consumer which consumes and ignores everything.
	 * Its acknowledgement completes when the supplier closes.
	 */
	static <T> StreamConsumer<T> skip() {
		return new Skip<>();
	}

	/**
	 * Creates a consumer which calls the provided {@link Consumer} with items
	 * it receives.
	 * Its acknowledgement completes when the supplier closes.
	 */
	static <T> StreamConsumer<T> of(Consumer<T> consumer) {
		return new StreamConsumers.OfConsumer<>(consumer);
	}

	/**
	 * Creates a consumer that is in the closed state with given error set.
	 */
	static <T> StreamConsumer<T> closingWithError(Throwable e) {
		return new ClosingWithError<>(e);
	}

	/**
	 * Creates a consumer that waits until the promise completes
	 * and then consumer items into the resulting consumer.
	 */
	static <T> StreamConsumer<T> ofPromise(Promise<? extends StreamConsumer<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new StreamConsumers.OfPromise<>(promise);
	}

	/**
	 * Creates a consumer that streams the received items into a given {@link io.datakernel.csp.ChannelConsumer channel consumer}
	 */
	static <T> StreamConsumer<T> ofChannelConsumer(ChannelConsumer<T> consumer) {
		return new OfChannelConsumer<>(consumer);
	}

	/**
	 * Creates a consumer which sends received items through the supplier received in the callback.
	 * Acknowledge of that consumer will not be set until the promise received from the callback invocation completes.
	 */
	static <T> StreamConsumer<T> ofSupplier(Function<StreamSupplier<T>, Promise<Void>> supplier) {
		StreamTransformer<T, T> forwarder = StreamTransformer.identity();
		Promise<Void> extraAcknowledge = supplier.apply(forwarder.getOutput());
		StreamConsumer<T> result = forwarder.getInput();
		if (extraAcknowledge == Promise.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	/**
	 * Transforms this supplier with a given transformer.
	 */
	default <R> R transformWith(StreamConsumerTransformer<T, R> fn) {
		return fn.transform(this);
	}

	/**
	 * Creates a consumer from this one with its <i>acknowledge</i> signal modified by the given function.
	 */
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
