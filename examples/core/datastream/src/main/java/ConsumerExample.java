import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of creating the custom StreamConsumer. This implementation just outputs received data to the console.
 */
public final class ConsumerExample<T> extends AbstractStreamConsumer<T> {
	@Override
	protected void onStarted() {
		getSupplier().resume(x -> System.out.println("received: " + x));
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		System.out.println("End of stream received");
		return Promise.complete();
	}

	@Override
	protected void onError(Throwable t) {
		System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamConsumer<Integer> consumer = new ConsumerExample<>();
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3);

		supplier.streamTo(consumer);

		eventloop.run();
	}
}
