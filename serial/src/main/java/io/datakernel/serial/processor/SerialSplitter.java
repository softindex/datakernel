/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.serial.processor;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.Preconditions.checkState;
import static io.datakernel.util.Recyclable.tryRecycle;
import static io.datakernel.util.Sliceable.trySlice;

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

	public static <T> SerialSplitter<T> create(SerialSupplier<T> input) {
		return new SerialSplitter<T>().withInput(input);
	}

	@Override
	public SerialInput<T> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure splitter while it is running");
			this.input = input;
			tryStart();
			return getProcessResult();
		};
	}

	@Override
	public SerialOutput<T> addOutput() {
		int index = outputs.size();
		outputs.add(null);
		return output -> {
			outputs.set(index, output);
			tryStart();
		};
	}

	private void tryStart() {
		if (this.input != null && this.outputs.stream().allMatch(Objects::nonNull)) {
			getCurrentEventloop().post(this::startProcess);
		}
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
							ack.thenComposeEx(($, e) -> {
								if (lenientExceptions.size() < outputs.size()) {
									outputs.remove(output);
									lenientExceptions.add(e);
									return Stage.complete();
								}
								lenientExceptions.forEach(e::addSuppressed);
								return Stage.ofException(e);
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
							Stages.all(outputs.stream().map(output -> output.accept(trySlice(item))))
									.whenComplete(($, e2) -> {
										if (e2 == null) {
											doProcess();
										} else {
											close(e2);
										}
									});
							tryRecycle(item);
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
