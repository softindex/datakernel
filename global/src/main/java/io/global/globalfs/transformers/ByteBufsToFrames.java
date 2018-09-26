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

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;
import io.global.globalfs.api.DataFrame;

import static io.datakernel.util.Preconditions.checkState;

abstract class ByteBufsToFrames extends AbstractAsyncProcess
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
	public MaterializedStage<Void> setInput(SerialSupplier<ByteBuf> input) {
		checkState(this.input == null, "Input is already set");
		this.input = input;
		return getResult();
	}

	@Override
	public void setOutput(SerialConsumer<DataFrame> output) {
		checkState(this.output == null, "Output is already set");
		this.output = output;
	}

	protected Stage<Void> postByteBuf(ByteBuf buf) {
		position += buf.readRemaining();
		return output.accept(DataFrame.of(buf));
	}

	protected abstract Stage<Void> postNextCheckpoint();

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

	protected abstract void iteration();

	@Override
	protected void doProcess() {
		postNextCheckpoint()
				.thenRun(this::iteration);
	}

	@Override
	protected final void doCloseWithError(Throwable e) {
		if (input != null) {
			input.closeWithError(e);
		}
		if (output != null) {
			output.closeWithError(e);
		}
	}
}
