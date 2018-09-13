package io.datakernel.stream;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;

import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public class TestStreamConsumers {
	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> decorator(Decorator<T> decorator) {
		return consumer -> new ForwardingStreamConsumer<T>(consumer) {
			final SettableStage<Void> acknowledgement = new SettableStage<>();

			{
				consumer.getAcknowledgement().whenComplete(acknowledgement::trySet);
			}

			@Override
			public void setProducer(StreamProducer<T> producer) {
				super.setProducer(new ForwardingStreamProducer<T>(producer) {
					@SuppressWarnings("unchecked")
					@Override
					public void produce(StreamDataReceiver<T> dataReceiver) {
						StreamDataReceiver<T>[] dataReceiverHolder = new StreamDataReceiver[1];
						Decorator.Context context = new Decorator.Context() {
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
								acknowledgement.trySetException(error);
							}
						};
						dataReceiverHolder[0] = decorator.decorate(context, dataReceiver);
						super.produce(dataReceiverHolder[0]);
					}
				});
			}

			@Override
			public MaterializedStage<Void> getAcknowledgement() {
				return acknowledgement;
			}

			@Override
			public void closeWithError(Throwable e) {
				super.closeWithError(e);
				acknowledgement.trySetException(e);
			}
		};
	}

	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> errorDecorator(Function<T, Throwable> errorFunction) {
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

	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> suspendDecorator(Predicate<T> predicate, Consumer<Decorator.Context> resumer) {
		return decorator((context, dataReceiver) ->
				item -> {
					dataReceiver.onData(item);

					if (predicate.test(item)) {
						context.suspend();
						resumer.accept(context);
					}
				});
	}

	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> suspendDecorator(Predicate<T> predicate) {
		return suspendDecorator(predicate, Decorator.Context::resume);
	}

	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> oneByOne() {
		return suspendDecorator(item -> true);
	}

	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> randomlySuspending(Random random, double probability) {
		return suspendDecorator(item -> random.nextDouble() < probability);
	}

	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> randomlySuspending(double probability) {
		return randomlySuspending(new Random(), probability);
	}

	public static <T> StreamConsumerModifier<T, StreamConsumer<T>> randomlySuspending() {
		return randomlySuspending(0.5);
	}

	public interface Decorator<T> {
		interface Context {
			void suspend();

			void resume();

			void closeWithError(Throwable error);
		}

		StreamDataReceiver<T> decorate(Context context, StreamDataReceiver<T> dataReceiver);
	}
}
