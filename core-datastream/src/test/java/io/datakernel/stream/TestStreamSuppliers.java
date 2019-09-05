package io.datakernel.stream;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.stream.TestStreamSuppliers.Decorator.Context;

import java.util.function.Function;

public class TestStreamSuppliers {
	public static <T> StreamSupplierTransformer<T, StreamSupplier<T>> decorator(Decorator<T> decorator) {
		return supplier -> new ForwardingStreamSupplier<T>(supplier) {
			final SettablePromise<Void> endOfStream = new SettablePromise<>();

			{
				supplier.getEndOfStream().whenComplete(endOfStream::trySet);
			}

			@Override
			public void resume(StreamDataAcceptor<T> dataAcceptor) {
				supplier.resume(decorator.decorate(new Context() {
					@Override
					public void endOfStream() {
						endOfStream.trySet(null);
					}

					@Override
					public void closeWithError(Throwable e) {
						endOfStream.trySetException(e);
					}
				}, dataAcceptor));
			}

			@Override
			public Promise<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamSupplierTransformer<T, StreamSupplier<T>> errorDecorator(Function<T, Throwable> errorFunction) {
		return decorator((context, dataAcceptor) ->
				item -> {
					Throwable e = errorFunction.apply(item);
					if (e == null) {
						dataAcceptor.accept(item);
					} else {
						context.closeWithError(e);
					}
				});
	}

	@FunctionalInterface
	public interface Decorator<T> {
		interface Context {
			void endOfStream();

			void closeWithError(Throwable e);
		}

		StreamDataAcceptor<T> decorate(Context context, StreamDataAcceptor<T> dataAcceptor);
	}
}
