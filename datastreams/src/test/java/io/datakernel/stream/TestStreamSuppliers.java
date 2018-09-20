package io.datakernel.stream;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;

import java.util.function.Function;

public class TestStreamSuppliers {
	public static <T> StreamSupplierFunction<T, StreamSupplier<T>> decorator(Decorator<T> decorator) {
		return supplier -> new ForwardingStreamSupplier<T>(supplier) {
			final SettableStage<Void> endOfStream = new SettableStage<>();

			{
				supplier.getEndOfStream().whenComplete(endOfStream::trySet);
			}

			@Override
			public void resume(StreamDataAcceptor<T> dataAcceptor) {
				supplier.resume(decorator.decorate(new Decorator.Context() {
					@Override
					public void endOfStream() {
						endOfStream.trySet(null);
					}

					@Override
					public void closeWithError(Throwable error) {
						endOfStream.trySetException(error);
					}
				}, dataAcceptor));
			}

			@Override
			public MaterializedStage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamSupplierFunction<T, StreamSupplier<T>> errorDecorator(Function<T, Throwable> errorFunction) {
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

	public interface Decorator<T> {
		interface Context {
			void endOfStream();

			void closeWithError(Throwable error);
		}

		StreamDataAcceptor<T> decorate(Context context, StreamDataAcceptor<T> dataAcceptor);
	}
}
