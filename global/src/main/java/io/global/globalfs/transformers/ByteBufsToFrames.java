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

package io.global.globalfs.transformers;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.AbstractIOAsyncProcess;
import io.datakernel.serial.processor.WithSerialToSerial;
import io.global.globalfs.api.DataFrame;

abstract class ByteBufsToFrames extends AbstractIOAsyncProcess
		implements WithSerialToSerial<ByteBufsToFrames, ByteBuf, DataFrame> {
	protected long position;
	protected long nextCheckpoint;

	protected SerialSupplier<ByteBuf> input;
	protected SerialConsumer<DataFrame> output;

	// region creators
	ByteBufsToFrames(long offset) {
		this.position = nextCheckpoint = offset;
	}
	// endregion

	@Override
	public SerialInput<ByteBuf> getInput() {
		return input -> {
			this.input = sanitize(input);
			return getResult();
		};
	}

	@Override
	public SerialOutput<DataFrame> getOutput() {
		return output -> this.output = sanitize(output);
	}

	@Override
	protected void doProcess() {
		postNextCheckpoint()
				.whenResult($ -> iteration());
	}

	@Override
	protected final void doCloseWithError(Throwable e) {
		input.close(e);
		output.close(e);
	}

	protected abstract Stage<Void> postNextCheckpoint();

	protected abstract void iteration();

	protected Stage<Void> postByteBuf(ByteBuf buf) {
		position += buf.readRemaining();
		return output.accept(DataFrame.of(buf));
	}

	protected Stage<Void> handleBuffer(ByteBuf buf) {
		int size = buf.readRemaining();

		if (position + size < nextCheckpoint) {
			return postByteBuf(buf);
		}

		int bytesUntilCheckpoint = (int) (nextCheckpoint - position);
		int remaining = size - bytesUntilCheckpoint;

		if (remaining == 0) {
			return postByteBuf(buf)
					.thenCompose($ -> postNextCheckpoint());
		}

		ByteBuf afterCheckpoint = buf.slice(buf.readPosition() + bytesUntilCheckpoint, remaining);
		buf.recycle(); // set refcount back to 1
		return postByteBuf(buf.slice(bytesUntilCheckpoint))
				.thenCompose($ -> postNextCheckpoint())
				.thenCompose($ -> handleBuffer(afterCheckpoint));
	}
}
