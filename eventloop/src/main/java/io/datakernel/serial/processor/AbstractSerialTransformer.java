/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.datakernel.serial.processor;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.MaterializedStage;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import static io.datakernel.util.Preconditions.checkState;

public abstract class AbstractSerialTransformer<S extends AbstractSerialTransformer<S, I, O>, I, O>
		extends AbstractAsyncProcess
		implements WithSerialToSerial<S, I, O> {
	protected SerialSupplier<I> input;
	protected SerialConsumer<O> output;

	@Override
	protected final void doCloseWithError(Throwable e) {
		if (input != null) {
			input.closeWithError(e);
		}
		if (output != null) {
			output.closeWithError(e);
		}
	}

	@Override
	public final void setOutput(SerialConsumer<O> output) {
		checkState(this.output == null, "Output already set");
		this.output = output;
	}

	@Override
	public final MaterializedStage<Void> setInput(SerialSupplier<I> input) {
		checkState(this.input == null, "Input already set");
		this.input = input;
		return getResult();
	}
}
