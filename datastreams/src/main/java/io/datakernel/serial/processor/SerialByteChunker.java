/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import io.datakernel.async.AsyncProcess;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.MemSize;

import static io.datakernel.bytebuf.ByteBufPool.pack;
import static java.lang.Math.min;

public final class SerialByteChunker implements AsyncProcess {
	private final SerialSupplier<ByteBuf> input;
	private final SerialConsumer<ByteBuf> output;

	private final int minChunkSize;
	private final int maxChunkSize;
	private final ByteBufQueue bufs = new ByteBufQueue();

	private SettableStage<Void> process;

	// region creators
	private SerialByteChunker(SerialSupplier<ByteBuf> input, SerialConsumer<ByteBuf> output,
			int minChunkSize, int maxChunkSize) {
		this.input = input;
		this.output = output;
		this.minChunkSize = minChunkSize;
		this.maxChunkSize = maxChunkSize;
	}

	public static SerialByteChunker create(SerialSupplier<ByteBuf> input, SerialConsumer<ByteBuf> output,
			MemSize minChunkSize, MemSize maxChunkSize) {
		return new SerialByteChunker(input, output,
				minChunkSize.toInt(), maxChunkSize.toInt());
	}
	// endregion

	public SerialSupplier<ByteBuf> getInput() {
		return input;
	}

	public SerialConsumer<ByteBuf> getOutput() {
		return output;
	}

	@Override
	public Stage<Void> process() {
		if (process == null) {
			process = new SettableStage<>();
			doProcess();
		}
		return process;
	}

	private void doProcess() {
		if (process.isComplete()) return;
		input.get()
				.async()
				.whenResult(buf -> {
					if (process.isComplete()) {
						buf.recycle();
						return;
					}
					if (buf == null) {
						output.accept(bufs.takeRemaining())
								.thenCompose($ -> output.accept(null))
								.thenRun(() -> process.trySet(null))
								.whenException(this::closeWithError);
						return;
					}
					bufs.add(pack(buf));
					if (!bufs.hasRemainingBytes(minChunkSize)) {
						doProcess();
						return;
					}

					int exactSize = 0;
					for (int i = 0; i != bufs.remainingBufs(); i++) {
						exactSize += bufs.peekBuf(i).readRemaining();
						if (exactSize >= minChunkSize) {
							break;
						}
					}
					ByteBuf out = bufs.takeExactSize(min(exactSize, maxChunkSize));
					output.accept(out)
							.async()
							.thenRun(this::doProcess)
							.whenException(this::closeWithError);
				})
				.whenException(this::closeWithError);
	}

	@Override
	public void closeWithError(Throwable e) {
		bufs.recycle();
		input.closeWithError(e);
		output.closeWithError(e);
		process.trySetException(e);
	}

}
