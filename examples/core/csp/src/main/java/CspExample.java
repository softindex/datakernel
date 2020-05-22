import io.datakernel.csp.*;
import io.datakernel.csp.dsl.WithChannelTransformer;
import io.datakernel.eventloop.Eventloop;

/**
 * AsyncProcess that takes a string, sets it to upper-case and adds string's length in parentheses
 */
//[START EXAMPLE]
public final class CspExample extends AbstractCommunicatingProcess implements WithChannelTransformer<CspExample, String, String> {
	private ChannelSupplier<String> input;
	private ChannelConsumer<String> output;

	@Override
	public ChannelOutput<String> getOutput() {
		return output -> {
			this.output = output;
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	public ChannelInput<String> getInput() {
		return input -> {
			this.input = input;
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@Override
	//[START REGION_1]
	protected void doProcess() {
		input.get()
				.whenComplete((data, e) -> {
					if (data == null) {
						output.acceptEndOfStream()
								.whenResult(this::completeProcess);
					} else {
						data = data.toUpperCase() + '(' + data.length() + ')';

						output.accept(data)
								.whenResult(this::doProcess);
					}
				});
	}
	//[END REGION_1]

	@Override
	protected void doClose(Throwable e) {
		System.out.println("Process has been closed with exception: " + e);
		input.closeEx(e);
		output.closeEx(e);
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		CspExample process = new CspExample();
		ChannelSupplier.of("hello", "world", "nice", "to", "see", "you")
				.transformWith(process)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));

		eventloop.run();
	}
}
//[END EXAMPLE]
