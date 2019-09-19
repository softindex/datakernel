/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.csp.process;

import io.datakernel.common.exception.Exceptions;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.ref.RefBoolean;
import io.datakernel.common.ref.RefInt;
import io.datakernel.csp.*;
import io.datakernel.csp.dsl.WithChannelInput;
import io.datakernel.csp.dsl.WithChannelOutputs;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.common.Recyclable.tryRecycle;
import static io.datakernel.common.Sliceable.trySlice;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class ChannelSplitter<T> extends AbstractCommunicatingProcess
		implements WithChannelInput<ChannelSplitter<T>, T>, WithChannelOutputs<ChannelSplitter<T>, T> {
	private ChannelSupplier<T> input;
	private final List<ChannelConsumer<T>> outputs = new ArrayList<>();

	private boolean lenient = false;
	private final List<Throwable> lenientExceptions = new ArrayList<>();

	private ChannelSplitter() {
	}

	public static <T> ChannelSplitter<T> create() {
		return new ChannelSplitter<>();
	}

	public static <T> ChannelSplitter<T> create(ChannelSupplier<T> input) {
		return new ChannelSplitter<T>().withInput(input);
	}

	public boolean hasOutputs() {
		return !outputs.isEmpty();
	}

	@Override
	public ChannelInput<T> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure splitter while it is running");
			this.input = sanitize(input);
			tryStart();
			return getProcessCompletion();
		};
	}

	@Override
	public ChannelOutput<T> addOutput() {
		int index = outputs.size();
		outputs.add(null);
		return output -> {
			outputs.set(index, sanitize(output));
			tryStart();
		};
	}

	public Promise<Void> splitInto(List<ChannelConsumer<T>> consumers, int requiredSuccesses, RefBoolean extraCondition) {
		RefInt up = new RefInt(consumers.size());

		consumers.forEach(output ->
				outputs.add(sanitize(output
						.withAcknowledgement(ack ->
								ack.whenException(e -> {
									if (e != null && up.dec() < requiredSuccesses && extraCondition.get()) {
										close(e);
									}
								})))));
		return startProcess()
				.then($ -> up.get() >= requiredSuccesses ?
						Promise.complete() :
						Promise.ofException(new StacklessException(ChannelSplitter.class, "Not enough successes")));
	}

	private void tryStart() {
		if (input != null && outputs.stream().allMatch(Objects::nonNull)) {
			getCurrentEventloop().post(this::startProcess);
		}
	}

	public void setLenient(boolean lenient) {
		checkState(!isProcessStarted(), "Can't configure splitter while it is running");
		this.lenient = lenient;
	}

	public ChannelSplitter<T> lenient() {
		setLenient(true);
		return this;
	}

	@Override
	protected void beforeProcess() {
		checkState(input != null, "No splitter input");
		checkState(!outputs.isEmpty(), "No splitter outputs");
		if (lenient) {
			outputs.replaceAll(output ->
					output.withAcknowledgement(ack ->
							ack.thenEx(($, e) -> {
								outputs.remove(output);
								lenientExceptions.add(e);
								if (!outputs.isEmpty()) {
									return Promise.complete();
								}
								return Promise.ofException(Exceptions.concat("All outputs were closed with exceptions", lenientExceptions));
							})));
		}
	}

	@Override
	protected void doProcess() {
		if (isProcessComplete()) {
			return;
		}
		input.get()
				.whenComplete((item, e) -> {
					if (e == null) {
						if (item != null) {
							Promises.all(outputs.stream().map(output -> output.accept(trySlice(item))))
									.whenComplete(($, e2) -> {
										if (e2 == null) {
											doProcess();
										} else {
											close(e2);
										}
									});
							tryRecycle(item);
						} else {
							Promises.all(outputs.stream().map(output -> output.accept(null)))
									.whenComplete(($, e1) -> completeProcess(e1));
						}
					} else {
						close(e);
					}
				});
	}

	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		outputs.forEach(output -> output.close(e));
	}
}
