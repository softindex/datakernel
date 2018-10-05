package io.datakernel.serial.processor;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.Stages;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

public final class SerialSplitter<T> extends AbstractAsyncProcess
		implements WithSerialInput<SerialSplitter<T>, T>, WithSerialOutputs<SerialSplitter<T>, T> {
	private SerialSupplier<T> input;
	private final List<SerialConsumer<T>> outputs = new ArrayList<>();

	private boolean lenient = false;
	private List<Throwable> lenientExceptions = new ArrayList<>();

	//region creators
	private SerialSplitter() {
	}

	public static <T> SerialSplitter<T> create() {
		return new SerialSplitter<>();
	}

	@Override
	public SerialInput<T> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure splitter while it is running");
			this.input = input;
			return getResult();
		};
	}

	@Override
	public SerialOutput<T> addOutput() {
		int index = outputs.size();
		outputs.add(null);
		return output -> outputs.set(index, output);
	}

	public void setLenient(boolean lenient) {
		checkState(!isProcessStarted(), "Can't configure splitter while it is running");
		this.lenient = lenient;
	}

	public SerialSplitter<T> lenient() {
		setLenient(true);
		return this;
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "No splitter input");
		checkState(!outputs.isEmpty(), "No splitter outputs");
		if (lenient) {
			outputs.replaceAll(output ->
					output.withAcknowledgement(ack ->
							ack.whenException(e -> {
								if (lenientExceptions.size() < outputs.size()) {
									lenientExceptions.add(e);
									return;
								}
								lenientExceptions.forEach(e::addSuppressed);
								close(e);
							})));
		}
	}

	@Override
	protected void doProcess() {
		if (isProcessComplete()) return;
		input.get()
				.whenComplete((item, e) -> {
					if (e == null) {
						if (item != null) {
							Stages.all(outputs.stream().map(output -> output.accept(item)))
									.whenComplete(($, e2) -> {
										if (e2 == null) {
											doProcess();
										} else if (!lenient) {
											close(e2);
										}
									});
						} else {
							Stages.all(outputs.stream().map(output -> output.accept(null)))
									.whenComplete(($, e1) -> completeProcess(e1));
						}
					} else {
						close(e);
					}
				});
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.close(e);
		outputs.forEach(output -> output.close(e));
	}
}
