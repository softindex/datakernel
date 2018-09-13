package io.datakernel.stream;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;

import java.util.function.Function;

public class TestStreamProducers {
	public static <T> StreamProducerModifier<T, StreamProducer<T>> decorator(Decorator<T> decorator) {
		return producer -> new ForwardingStreamProducer<T>(producer) {
			final SettableStage<Void> endOfStream = new SettableStage<>();

			{
				producer.getEndOfStream().whenComplete(endOfStream::trySet);
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				producer.produce(decorator.decorate(new Decorator.Context() {
					@Override
					public void endOfStream() {
						endOfStream.trySet(null);
					}

					@Override
					public void closeWithError(Throwable error) {
						endOfStream.trySetException(error);
					}
				}, dataReceiver));
			}

			@Override
			public MaterializedStage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamProducerModifier<T, StreamProducer<T>> errorDecorator(Function<T, Throwable> errorFunction) {
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

	public interface Decorator<T> {
		interface Context {
			void endOfStream();

			void closeWithError(Throwable error);
		}

		StreamDataReceiver<T> decorate(Context context, StreamDataReceiver<T> dataReceiver);
	}
}
