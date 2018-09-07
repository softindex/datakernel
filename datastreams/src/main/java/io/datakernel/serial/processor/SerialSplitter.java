package io.datakernel.serial.processor;

import io.datakernel.async.AsyncProcess;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

public class SerialSplitter<T> implements AsyncProcess, WithSerialInput<SerialSplitter<T>, T>, WithSerialOutputs<SerialSplitter<T>, T> {
	private SerialSupplier<T> input;
	private final List<SerialConsumer<T>> outputs = new ArrayList<>();

	private boolean lenient = false;
	private List<Throwable> lenientExceptions = new ArrayList<>();
	private SettableStage<Void> process;

	//region creators
	private SerialSplitter() {
	}

	public static <T> SerialSplitter<T> create() {
		return new SerialSplitter<>();
	}

	@Override
	public void setInput(SerialSupplier<T> input) {
		checkState(process == null, "Can't comfigure splitter while it is running");
		this.input = input;
	}

	@Override
	public SerialOutput<T> addOutput() {
		int index = outputs.size();
		outputs.add(null);
		return output -> outputs.set(index, output);
	}

	public void setLenient(boolean lenient) {
		checkState(process == null, "Can't configure splitter while it is running");
		this.lenient = lenient;
	}

	public SerialSplitter<T> lenient() {
		setLenient(true);
		return this;
	}
	// endregion

	@Override
	public Stage<Void> process() {
		checkState(input != null, "No splitter input");
		checkState(!outputs.isEmpty(), "No splitter outputs");

		if (process == null) {
			process = new SettableStage<>();
			doProcess();
		}
		return process;
	}

	private void doProcess() {
		if (process.isComplete()) {
			return;
		}
		if (lenient) {
			outputs.replaceAll(o ->
					o.whenException(e -> {
						if (lenientExceptions.size() < outputs.size()) {
							lenientExceptions.add(e);
							return;
						}
						lenientExceptions.forEach(e::addSuppressed);
						closeWithError(e);
					}));
		}
		input.get()
				.whenComplete((item, e) -> {
					if (e != null) {
						closeWithError(e);
						return;
					}
					if (item == null) {
						Stages.all(outputs.stream().map(o -> o.accept(null)))
								.whenComplete(process::set);
						return;
					}
					Stages.all(outputs.stream().map(o -> o.accept(item)))
							.async()
							.whenComplete(($, e2) -> {
								if (e2 == null) {
									doProcess();
								} else if (!lenient) {
									closeWithError(e2);
								}
							});
				});

	}

	@Override
	public void closeWithError(Throwable e) {
		input.closeWithError(e);
		outputs.forEach(o -> o.closeWithError(e));
		process.trySetException(e);
	}
}
