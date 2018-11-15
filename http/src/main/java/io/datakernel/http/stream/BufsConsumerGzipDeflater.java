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

package io.datakernel.http.stream;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;

import java.util.zip.CRC32;
import java.util.zip.Deflater;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;

public final class BufsConsumerGzipDeflater extends AbstractAsyncProcess
		implements WithSerialToSerial<BufsConsumerGzipDeflater, ByteBuf, ByteBuf> {
	public static final int DEFAULT_MAX_BUF_SIZE = 512;
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
	private static final int GZIP_FOOTER_SIZE = 8;

	private final CRC32 crc32 = new CRC32();

	private Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
	private int maxBufSize = DEFAULT_MAX_BUF_SIZE;
	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerGzipDeflater() {}

	public static BufsConsumerGzipDeflater create() {
		return new BufsConsumerGzipDeflater();
	}

	public BufsConsumerGzipDeflater withDeflater(Deflater deflater) {
		checkArgument(deflater != null, "Cannot use null Deflater");
		this.deflater = deflater;
		return this;
	}

	public BufsConsumerGzipDeflater withMaxBufSize(int maxBufSize) {
		checkArgument(maxBufSize > 0, "Cannot use buf size that is less than 0");
		this.maxBufSize = maxBufSize;
		return this;
	}

	@SuppressWarnings("ConstantConditions") //check input for clarity
	@Override
	public SerialInput<ByteBuf> getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			if (this.input != null && this.output != null) startProcess();
			return getProcessResult();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public SerialOutput<ByteBuf> getOutput() {
		return output -> {
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		writeHeader();
	}

	private void writeHeader() {
		output.accept(ByteBuf.wrapForReading(GZIP_HEADER))
				.whenResult($ -> writeBody());
	}

	private void writeBody() {
		input.get()
				.whenComplete((buf, e) -> {
					if (buf != null) {
						if (buf.canRead()) {
							crc32.update(buf.array(), buf.readPosition(), buf.readRemaining());
							deflater.setInput(buf.array(), buf.readPosition(), buf.readRemaining());
							buf.recycle();
							ByteBufQueue queue = deflate();
							output.acceptAll(queue.asIterator())
									.whenResult($ -> writeBody());
						} else {
							buf.recycle();
						}
					} else {
						writeFooter();
					}
				});
	}

	private void writeFooter() {
		deflater.finish();
		ByteBufQueue queue = deflate();
		ByteBuf footer = ByteBufPool.allocate(GZIP_FOOTER_SIZE);
		footer.writeInt(Integer.reverseBytes((int) crc32.getValue()));
		footer.writeInt(Integer.reverseBytes(deflater.getTotalIn()));
		queue.add(footer);
		output.acceptAll(queue.asIterator())
				.thenCompose($ -> output.accept(null))
				.whenResult($ -> completeProcess());
	}

	private ByteBufQueue deflate() {
		ByteBufQueue queue = new ByteBufQueue();
		while (true) {
			ByteBuf out = ByteBufPool.allocate(maxBufSize);
			int len = deflater.deflate(out.array(), out.writePosition(), out.writeRemaining());
			if (len > 0) {
				out.writePosition(len);
				queue.add(out);
			} else {
				out.recycle();
				return queue;
			}
		}
	}

	@Override
	protected void doClose(Throwable e) {
		deflater.end();
		input.close(e);
		output.close(e);
	}
}
