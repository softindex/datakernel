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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.InvalidSizeException;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.ByteBufsInput;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.processor.WithByteBufsInput;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.serial.ByteBufsParser.assertBytes;
import static io.datakernel.serial.ByteBufsParser.ofCrlfTerminatedBytes;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.Math.min;

public final class BufsConsumerChunkedDecoder extends AbstractAsyncProcess
		implements WithSerialToSerial<BufsConsumerChunkedDecoder, ByteBuf, ByteBuf>, WithByteBufsInput<BufsConsumerChunkedDecoder> {
	public static final int DEFAULT_MAX_EXT_LENGTH = 1024; //1 Kb
	public static final int DEFAULT_MAX_CHUNK_LENGTH = 1024; //1 Kb
	public static final int MAX_CHUNK_LENGTH_DIGITS = 8;
	public static final byte[] CRLF = {13, 10};
	// region exceptions
	public static final ParseException MALFORMED_CHUNK = new ParseException(BufsConsumerChunkedDecoder.class, "Malformed chunk");
	public static final ParseException MALFORMED_CHUNK_LENGTH = new InvalidSizeException(BufsConsumerChunkedDecoder.class, "Malformed chunk length");
	public static final ParseException EXT_TOO_LARGE = new InvalidSizeException(BufsConsumerChunkedDecoder.class, "Malformed chunk, chunk-ext is larger than maximum allowed length");
	public static final ParseException TRAILER_TOO_LARGE = new InvalidSizeException(BufsConsumerChunkedDecoder.class, "Malformed chunk, trailer-part is larger than maximum allowed length");
	// endregion

	private int maxExtLength = DEFAULT_MAX_EXT_LENGTH;
	private int maxChunkLength = DEFAULT_MAX_CHUNK_LENGTH;

	private ByteBufQueue bufs;
	private ByteBufsSupplier input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerChunkedDecoder() {}

	public static BufsConsumerChunkedDecoder create() {
		return new BufsConsumerChunkedDecoder();
	}

	public BufsConsumerChunkedDecoder withMaxChunkLength(int maxChunkLength) {
		this.maxChunkLength = maxChunkLength;
		return this;
	}

	public BufsConsumerChunkedDecoder withMaxExtLength(int maxExtLength) {
		this.maxExtLength = maxExtLength;
		return this;
	}

	@Override
	public ByteBufsInput getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			this.bufs = input.getBufs();
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
		processLength();
	}

	private void processLength() {
		input.parse(
				queue -> {
					int remainingBytes = queue.remainingBytes();
					int chunkLength = 0;
					for (int i = 0; i < min(remainingBytes, MAX_CHUNK_LENGTH_DIGITS); i++) {
						byte c = queue.peekByte(i);
						if (c >= '0' && c <= '9') {
							chunkLength = (chunkLength << 4) + (c - '0');
						} else if (c >= 'a' && c <= 'f') {
							chunkLength = (chunkLength << 4) + (c - 'a' + 10);
						} else if (c >= 'A' && c <= 'F') {
							chunkLength = (chunkLength << 4) + (c - 'A' + 10);
						} else if (c == ';' || c == CR) {
							// Success
							if (i == 0 || chunkLength > maxChunkLength || chunkLength < 0) {
								throw MALFORMED_CHUNK_LENGTH;
							}
							queue.skip(i);
							return chunkLength;
						} else {
							throw MALFORMED_CHUNK_LENGTH;
						}
					}

					if (remainingBytes > MAX_CHUNK_LENGTH_DIGITS) {
						throw MALFORMED_CHUNK;
					}

					return null;
				})
				.whenResult(chunkLength -> {
					if (chunkLength != 0) {
						consumeCRLF(chunkLength);
					} else {
						validateLastChunk();
					}
				});
	}

	private void processData(int chunkLength, ByteBufQueue queue) {
		chunkLength -= bufs.drainTo(queue, chunkLength);
		if (chunkLength != 0) {
			int newChunkLength = chunkLength;
			input.needMoreData()
					.whenResult($ -> processData(newChunkLength, queue));
			return;
		}
		input.parse(assertBytes(CRLF))
				.whenException(e -> {
					queue.recycle();
					close(MALFORMED_CHUNK);
				})
				.thenCompose($ -> output.acceptAll(queue.asIterator()))
				.whenResult($ -> processLength());
	}

	private void consumeCRLF(int chunkLength) {
		input.parse(ofCrlfTerminatedBytes(maxExtLength))
				.whenResult(ByteBuf::recycle)
				.whenException(e -> close(EXT_TOO_LARGE))
				.whenResult($ -> processData(chunkLength, new ByteBufQueue()));
	}

	private void validateLastChunk() {
		int remainingBytes = bufs.remainingBytes();
		for (int i = 0; i < min(maxExtLength, remainingBytes - 3); i++) {
			if (bufs.peekByte(i) == CR
					&& bufs.peekByte(i + 1) == LF
					&& bufs.peekByte(i + 2) == CR
					&& bufs.peekByte(i + 3) == LF) {
				bufs.skip(i + 4);

				input.endOfStream()
						.thenCompose($ -> output.accept(null))
						.whenResult($ -> completeProcess());
				return;
			}
		}

		if (remainingBytes > maxExtLength) {
			close(TRAILER_TOO_LARGE);
			return;
		}
		input.needMoreData()
				.whenResult($ -> validateLastChunk());
	}

	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		output.close(e);
	}
}
