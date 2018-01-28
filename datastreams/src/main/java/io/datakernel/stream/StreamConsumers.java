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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers.Decorator.Context;

import java.util.EnumSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.async.Stages.onError;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.stream.StreamCapability.TERMINAL;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final Throwable exception;
		private final SettableStage<Void> endOfStream = SettableStage.create();

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setProducer(StreamProducer<T> producer) {
			getCurrentEventloop().post(() -> endOfStream.trySetException(exception));
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING, TERMINAL);
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	static final class IdleImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		@Override
		public void setProducer(StreamProducer<T> producer) {
			producer.getEndOfStream()
					.whenComplete(Stages.onResult(endOfStream::trySet))
					.whenComplete(onError(endOfStream::trySetException));
		}

		@Override
		public CompletionStage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING, TERMINAL);
		}
	}

	public interface Decorator<T> {
		interface Context {
			void suspend();

			void resume();

			void closeWithError(Throwable error);
		}

		StreamDataReceiver<T> decorate(Context context, StreamDataReceiver<T> dataReceiver);
	}

	public static <T> StreamConsumerModifier<T, T> decorator(Decorator<T> decorator) {
		return consumer -> new ForwardingStreamConsumer<T>(consumer) {
			private final SettableStage<Void> endOfStream = SettableStage.mirrorOf(consumer.getEndOfStream());

			@Override
			public void setProducer(StreamProducer<T> producer) {
				super.setProducer(new ForwardingStreamProducer<T>(producer) {
					@SuppressWarnings("unchecked")
					@Override
					public void produce(StreamDataReceiver<T> dataReceiver) {
						StreamDataReceiver<T>[] dataReceiverHolder = new StreamDataReceiver[1];
						Context context = new Context() {
							final Eventloop eventloop = getCurrentEventloop();

							@Override
							public void suspend() {
								producer.suspend();
							}

							@Override
							public void resume() {
								eventloop.post(() -> producer.produce(dataReceiverHolder[0]));
							}

							@Override
							public void closeWithError(Throwable error) {
								endOfStream.trySetException(error);
							}
						};
						dataReceiverHolder[0] = decorator.decorate(context, dataReceiver);
						super.produce(dataReceiverHolder[0]);
					}
				});
			}

			@Override
			public CompletionStage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamConsumerModifier<T, T> errorDecorator(Function<T, Throwable> errorFunction) {
		return decorator((context, dataReceiver) ->
				item -> {
					Throwable error = errorFunction.apply(item);
					if (error == null) {
						dataReceiver.onData(item);
					} else {
						context.closeWithError(error);
					}
				});
	}

	public static <T> StreamConsumerModifier<T, T> suspendDecorator(Predicate<T> predicate, Consumer<Context> resumer) {
		return decorator((context, dataReceiver) ->
				item -> {
					dataReceiver.onData(item);

					if (predicate.test(item)) {
						context.suspend();
						resumer.accept(context);
					}
				});
	}

	public static <T> StreamConsumerModifier<T, T> suspendDecorator(Predicate<T> predicate) {
		return suspendDecorator(predicate, Context::resume);
	}

	public static <T> StreamConsumerModifier<T, T> oneByOne() {
		return suspendDecorator(item -> true);
	}

	public static <T> StreamConsumerModifier<T, T> randomlySuspending(Random random, double probability) {
		return suspendDecorator(item -> random.nextDouble() < probability);
	}

	public static <T> StreamConsumerModifier<T, T> randomlySuspending(double probability) {
		return randomlySuspending(new Random(), probability);
	}

	public static <T> StreamConsumerModifier<T, T> randomlySuspending() {
		return randomlySuspending(0.5);
	}

}
