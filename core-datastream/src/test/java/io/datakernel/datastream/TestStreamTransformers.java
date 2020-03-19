package io.datakernel.datastream;

import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Random;
import java.util.function.Function;

public class TestStreamTransformers {
	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> oneByOne() {
		return decorate(Promise::async);
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> randomlySuspending() {
		return randomlySuspending(new Random(), 0.5);
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> randomlySuspending(Random rnd, double probability) {
		return decorate(promise -> {
			double b = rnd.nextDouble();
			return b < probability ? promise : promise.async();
		});
	}

	public static <T> StreamConsumerTransformer<T, StreamConsumer<T>> decorate(Function<Promise<T>, Promise<T>> fn) {
		return consumer ->
				StreamConsumer.ofChannelConsumer(
						asStreamConsumer(consumer)
								.transformWith(channelConsumer -> new AbstractChannelConsumer<T>(channelConsumer) {
									@Override
									protected Promise<Void> doAccept(@Nullable T value) {
										return fn.apply(channelConsumer.accept(value).map($ -> value)).toVoid();
									}
								}));
	}

	static <T> ChannelConsumer<T> asStreamConsumer(StreamConsumer<T> consumer) {
		return new AsChannelConsumer<>(consumer);
	}

	static final class AsChannelConsumer<T> extends AbstractChannelConsumer<T> {
		private final AbstractStreamSupplier<T> internalSupplier = new AbstractStreamSupplier<T>() {};

		AsChannelConsumer(StreamConsumer<T> consumer) {
			this.internalSupplier.streamTo(consumer);

			consumer.getAcknowledgement()
					.whenResult(this::close)
					.whenException(this::closeEx);
		}

		@Override
		protected Promise<Void> doAccept(@Nullable T item) {
			if (item == null) {
				internalSupplier.sendEndOfStream();
				return internalSupplier.getConsumer().getAcknowledgement();
			}
			internalSupplier.send(item);
			return internalSupplier.getFlushPromise();
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			internalSupplier.closeEx(e);
		}
	}

}
