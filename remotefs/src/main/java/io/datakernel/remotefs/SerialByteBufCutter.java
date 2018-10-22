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

package io.datakernel.remotefs;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.util.Preconditions.checkState;

public class SerialByteBufCutter extends AbstractAsyncProcess
		implements WithSerialToSerial<SerialByteBufCutter, ByteBuf, ByteBuf> {
	private final long offset;

	private long position = 0;

	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private SerialByteBufCutter(long offset) {
		this.offset = offset;
	}

	public static SerialByteBufCutter create(long offset) {
		return new SerialByteBufCutter(offset);
	}

	@Override
	public SerialInput<ByteBuf> getInput() {
		return input -> {
			this.input = sanitize(input);
			if (this.output != null) startProcess();
			return getProcessResult();
		};
	}

	@Override
	public SerialOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = sanitize(output);
			if (this.input != null) startProcess();
		};
	}

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(item -> {
					if (item != null) {
						int size = item.readRemaining();
						position += size;
						if (position <= offset) {
							item.recycle();
							doProcess();
							return;
						}
						if (position - size < offset) {
							item.moveReadPosition(size - (int) (position - offset));
						}
						output.accept(item)
								.whenResult(($) -> doProcess());
					} else {
						output.accept(null)
								.whenComplete(($, e2) -> completeProcess(e2));
					}
				});
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.close(e);
		output.close(e);
	}
}
