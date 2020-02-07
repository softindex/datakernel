import io.datakernel.datastream.*;
import io.datakernel.datastream.processor.StreamTransformer;
import io.datakernel.eventloop.Eventloop;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of creating custom StreamTransformer, which takes strings from input stream
 * and transforms strings to their length if particular length is less than MAX_LENGTH
 */
@SuppressWarnings("Convert2MethodRef")
public final class TransformerExample implements StreamTransformer<String, Integer> {
	private static final int MAX_LENGTH = 10;

	//[START REGION_1]
	private final AbstractStreamConsumer<String> input = new AbstractStreamConsumer<String>() {
		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}
	};

	private final AbstractStreamSupplier<Integer> output = new AbstractStreamSupplier<Integer>() {
		@Override
		protected void onResumed(AsyncProduceController async) {
			input.resume(item -> {
				int len = item.length();
				if (len < MAX_LENGTH) {
					output.send(len);
				}
			});
		}

		@Override
		protected void onSuspended() {
			input.suspend();
		}
	};
	//[END REGION_1]

	{
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getEndOfStream()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}


	@Override
	public StreamConsumer<String> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<Integer> getOutput() {
		return output;
	}

	//[START REGION_2]
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamSupplier<String> source = StreamSupplier.of("testdata", "testdata1", "testdata1000");
		TransformerExample transformer = new TransformerExample();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		source.transformWith(transformer).streamTo(consumer);
		consumer.getResult().whenResult(v -> System.out.println(v));

		eventloop.run();
	}
	//[END REGION_2]
}

