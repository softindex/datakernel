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

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.MemSize;

import static java.lang.Math.min;

public final class SerialByteChunker extends AbstractAsyncProcess
		implements WithSerialToSerial<SerialByteChunker, ByteBuf, ByteBuf> {
	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	private final int minChunkSize;
	private final int maxChunkSize;
	private final ByteBufQueue bufs = new ByteBufQueue();

	// region creators
	private SerialByteChunker(int minChunkSize, int maxChunkSize) {
		this.minChunkSize = minChunkSize;
		this.maxChunkSize = maxChunkSize;
	}

	public static SerialByteChunker create(MemSize minChunkSize, MemSize maxChunkSize) {
		return new SerialByteChunker(minChunkSize.toInt(), maxChunkSize.toInt());
	}

	@Override
	public MaterializedStage<Void> setInput(SerialSupplier<ByteBuf> input) {
		this.input = input;
		return getResult();
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		this.output = output;
	}
	// endregion

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (buf != null) {
						bufs.add(buf);
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
								.whenResult($ -> doProcess());
					} else {
						Stage.complete()
								.thenCompose($ -> bufs.hasRemaining() ? output.accept(bufs.takeRemaining()) : Stage.complete())
								.thenCompose($ -> output.accept(null))
								.whenResult($1 -> completeProcess());
					}
				});
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		bufs.recycle();
		input.close(e);
		output.close(e);
	}

}
