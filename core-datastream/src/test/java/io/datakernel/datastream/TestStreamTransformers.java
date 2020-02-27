package io.datakernel.datastream;

import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Random;
import java.util.function.Function;

import static io.datakernel.common.Preconditions.checkState;

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
		private final StreamConsumer<T> streamConsumer;
		private StreamDataAcceptor<T> dataAcceptor;
		private T item;
		private SettablePromise<Void> itemPromise;
		final SettablePromise<Void> endOfStream = new SettablePromise<>();

		AsChannelConsumer(StreamConsumer<T> consumer) {
			streamConsumer = consumer;
			streamConsumer.getAcknowledgement()
					.whenResult(this::close)
					.whenException(this::closeEx);
			if (!streamConsumer.getAcknowledgement().isComplete()) {
				streamConsumer.consume(new StreamSupplier<T>() {
					@Override
					public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
						if (isClosed()) return;
						if (streamConsumer.getAcknowledgement().isComplete()) return;
						AsChannelConsumer.this.dataAcceptor = dataAcceptor;
						if (dataAcceptor != null) {
							if (item != null) {
								dataAcceptor.accept(item);
								itemPromise.set(null);
							}
						}
					}

					@Override
					public Promise<Void> getEndOfStream() {
						return endOfStream;
					}

					@Override
					public void closeEx(@NotNull Throwable e) {
					}
				});
			}
		}

		@Override
		protected Promise<Void> doAccept(@Nullable T item) {
			assert !isClosed();
			assert !streamConsumer.getAcknowledgement().isComplete();
			if (item == null) {
				endOfStream.trySet(null);
				return streamConsumer.getAcknowledgement();
			}
			if (dataAcceptor != null) {
				dataAcceptor.accept(item);
				return Promise.complete();
			} else {
				this.item = item;
				this.itemPromise = new SettablePromise<>();
				this.itemPromise.whenComplete(() -> { // *
					this.item = null;
					this.itemPromise = null;
				});
				return this.itemPromise;
			}
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			dataAcceptor = null;
			endOfStream.trySetException(e);
			streamConsumer.closeEx(e);
			if (itemPromise != null) {
				itemPromise.setException(e); // *
			}
			checkState(this.dataAcceptor == null);
			checkState(this.item == null);
		}
	}

}
