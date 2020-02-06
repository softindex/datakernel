import io.datakernel.datastream.AbstractStreamConsumer;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of creating custom StreamConsumer. This implementation outputs received data to the console.
 */
//[START EXAMPLE]
public final class ConsumerExample<T> extends AbstractStreamConsumer<T> {
	@Override
	protected void onStarted() {
		resume(x -> System.out.println("received: " + x));
	}

	@Override
	protected void onEndOfStream() {
		System.out.println("End of stream received");
		acknowledge();
	}

	@Override
	protected void onError(Throwable t) {
		System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
	}
//[END EXAMPLE]

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamConsumer<Integer> consumer = new ConsumerExample<>();
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3);

		supplier.streamTo(consumer);

		eventloop.run();
	}
}
