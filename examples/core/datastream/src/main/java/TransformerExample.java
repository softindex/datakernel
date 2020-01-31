import io.datakernel.datastream.*;
import io.datakernel.datastream.processor.StreamTransformer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of creating custom StreamTransformer, which takes strings from input stream
 * and transforms strings to their length if particular length is less than MAX_LENGTH
 */
public final class TransformerExample implements StreamTransformer<String, Integer> {
	private static final int MAX_LENGTH = 10;

	//[START REGION_1]
	private final AbstractStreamConsumer<String> inputConsumer = new AbstractStreamConsumer<String>() {

		@Override
		protected Promise<Void> onEndOfStream() {
			outputSupplier.sendEndOfStream();
			return Promise.complete();
		}

		@Override
		protected void onError(Throwable t) {
			System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
		}
	};

	private final AbstractStreamSupplier<Integer> outputSupplier = new AbstractStreamSupplier<Integer>() {

		@Override
		protected void onSuspended() {
			inputConsumer.getSupplier().suspend();
		}

		@Override
		protected void produce(AsyncProduceController async) {
			inputConsumer.getSupplier()
					.resume(item -> {
						int len = item.length();
						if (len < MAX_LENGTH) {
							send(len);
						}
					});
		}

		@Override
		protected void onError(Throwable t) {
			System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
		}
	};
	//[END REGION_1]

	@Override
	public StreamConsumer<String> getInput() {
		return inputConsumer;
	}

	@Override
	public StreamSupplier<Integer> getOutput() {
		return outputSupplier;
	}

	//[START REGION_2]
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamSupplier<String> source = StreamSupplier.of("testdata", "testdata1", "testdata1000");
		TransformerExample transformer = new TransformerExample();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		source.transformWith(transformer).streamTo(consumer);
		consumer.getResult().whenResult(System.out::println);

		eventloop.run();
	}
	//[END REGION_2]
}

