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
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers.Decorator.Context;

import java.util.EnumSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final Throwable exception;
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setProducer(StreamProducer<T> producer) {
			getCurrentEventloop().post(() -> endOfStream.setException(exception));
		}

		@Override
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	static final class OfConsumerImpl<T> extends AbstractStreamConsumer<T> {
		private final Consumer<T> consumer;

		OfConsumerImpl(Consumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			getProducer().produce(consumer::accept);
		}

		@Override
		protected void onEndOfStream() {

		}

		@Override
		protected void onError(Throwable t) {

		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	static final class OfAsyncConsumerImpl<T> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, Void>, StreamDataReceiver<T> {
		private final AsyncConsumer<T> consumer;
		private int waiting;
		private final SettableStage<Void> resultStage = new SettableStage<>();

		OfAsyncConsumerImpl(AsyncConsumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@Override
		public void onData(T item) {
			Stage<Void> stage = consumer.accept(item);
			if (stage instanceof SettableStage) {
				SettableStage<Void> settableStage = (SettableStage<Void>) stage;
				if (settableStage.isSetResult()) {
					// do nothing, continue streaming
					return;
				} else if (settableStage.isSetException()) {
					closeWithError(settableStage.getException());
					return;
				}
			}
			waiting++;
			getProducer().suspend();
			stage.whenComplete(($, throwable) -> {
				if (--waiting == 0) {
					if (throwable == null) {
						if (getStatus().isOpen()) {
							getProducer().streamTo(this);
						} else {
							resultStage.trySet(null);
						}
					} else {
						closeWithError(throwable);
					}
				}
			});
		}

		@Override
		protected void onEndOfStream() {
			if (waiting == 0) {
				resultStage.trySet(null);
			}
		}

		@Override
		protected void onError(Throwable t) {
			resultStage.trySetException(t);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public Stage<Void> getResult() {
			return null;
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	static final class IdleImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> endOfStream = new SettableStage<>();

		@Override
		public void setProducer(StreamProducer<T> producer) {
			producer.getEndOfStream().whenComplete(endOfStream::set);
			producer.produce($ -> {});
		}

		@Override
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
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
			final SettableStage<Void> endOfStream = new SettableStage<>();

			{
				consumer.getEndOfStream().whenComplete(endOfStream::trySet);
			}

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
			public Stage<Void> getEndOfStream() {
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
