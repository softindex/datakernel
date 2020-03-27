package io.datakernel.datastream;

import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.datastream.visitor.StreamVisitor;
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
		return c -> {
			AsChannelConsumer<T> channel = asChannelConsumer(c);
			StreamConsumer<T> decorated = StreamConsumer.ofChannelConsumer(channel
					.transformWith(channelConsumer -> new AbstractChannelConsumer<T>(channelConsumer) {
						@Override
						protected Promise<Void> doAccept(@Nullable T value) {
							return fn.apply(channelConsumer.accept(value).map($ -> value)).toVoid();
						}
					}));
			return new ForwardingStreamConsumer<T>(decorated, "TestStreamTransformers.decorate") {
				@Override
				public void accept(StreamVisitor visitor) {
					super.accept(visitor);
					c.accept(visitor);
					visitor.visitImplicit(decorated, channel.internalSupplier);
				}
			};
		};
	}

	static <T> AsChannelConsumer<T> asChannelConsumer(StreamConsumer<T> consumer) {
		return new AsChannelConsumer<>(consumer);
	}

	static final class AsChannelConsumer<T> extends AbstractChannelConsumer<T> {
		private final AbstractStreamSupplier<T> internalSupplier = new AbstractStreamSupplier<T>() {
			@Override
			public String getLabel() {
				return "AsChannelConsumer.internalSupplier";
			}
		};

		AsChannelConsumer(StreamConsumer<T> consumer) {
			internalSupplier.streamTo(consumer);

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
