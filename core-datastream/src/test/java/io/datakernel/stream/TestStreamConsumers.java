package io.datakernel.stream;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.TestStreamConsumers.Decorator.Context;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public class TestStreamConsumers {
	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> decorator(Decorator<T> decorator) {
		return consumer -> new ForwardingStreamConsumer<T>(consumer) {
			final SettablePromise<Void> acknowledgement = new SettablePromise<>();

			{
				consumer.getAcknowledgement().acceptEx(acknowledgement::trySet);
			}

			@Override
			public void setSupplier(StreamSupplier<T> supplier) {
				super.setSupplier(new ForwardingStreamSupplier<T>(supplier) {
					@SuppressWarnings("unchecked")
					@Override
					public void resume(StreamDataAcceptor<T> dataAcceptor) {
						StreamDataAcceptor<T>[] dataAcceptors = new StreamDataAcceptor[1];
						Context context = new Context() {
							final Eventloop eventloop = getCurrentEventloop();

							@Override
							public void suspend() {
								supplier.suspend();
							}

							@Override
							public void resume() {
								eventloop.post(() -> supplier.resume(dataAcceptors[0]));
							}

							@Override
							public void closeWithError(Throwable e) {
								acknowledgement.trySetException(e);
							}
						};
						dataAcceptors[0] = decorator.decorate(context, dataAcceptor);
						super.resume(dataAcceptors[0]);
					}
				});
			}

			@Override
			public MaterializedPromise<Void> getAcknowledgement() {
				return acknowledgement;
			}

			@Override
			public void close(@NotNull Throwable e) {
				super.close(e);
				acknowledgement.trySetException(e);
			}
		};
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> errorDecorator(Function<T, Throwable> errorFunction) {
		return decorator((context, dataAcceptor) ->
				item -> {
					Throwable error = errorFunction.apply(item);
					if (error == null) {
						dataAcceptor.accept(item);
					} else {
						context.closeWithError(error);
					}
				});
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> suspendDecorator(Predicate<T> predicate, Consumer<Context> resumer) {
		return decorator((context, dataAcceptor) ->
				item -> {
					dataAcceptor.accept(item);

					if (predicate.test(item)) {
						context.suspend();
						resumer.accept(context);
					}
				});
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> suspendDecorator(Predicate<T> predicate) {
		return suspendDecorator(predicate, Context::resume);
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> oneByOne() {
		return suspendDecorator(item -> true);
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> randomlySuspending(Random random, double probability) {
		return suspendDecorator(item -> random.nextDouble() < probability);
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> randomlySuspending(double probability) {
		return randomlySuspending(new Random(), probability);
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> randomlySuspending() {
		return randomlySuspending(0.5);
	}

	@FunctionalInterface
	public interface Decorator<T> {
		interface Context {
			void suspend();

			void resume();

			void closeWithError(Throwable e);
		}

		StreamDataAcceptor<T> decorate(Context context, StreamDataAcceptor<T> dataAcceptor);
	}
}
