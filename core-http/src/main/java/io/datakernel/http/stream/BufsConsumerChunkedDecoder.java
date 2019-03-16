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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.AbstractCommunicatingProcess;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.binary.BinaryChannelInput;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.dsl.WithBinaryChannelInput;
import io.datakernel.csp.dsl.WithChannelTransformer;
import io.datakernel.exception.InvalidSizeException;
import io.datakernel.exception.ParseException;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.csp.binary.ByteBufsParser.assertBytes;
import static io.datakernel.csp.binary.ByteBufsParser.ofCrlfTerminatedBytes;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.Math.min;

public final class BufsConsumerChunkedDecoder extends AbstractCommunicatingProcess
		implements WithChannelTransformer<BufsConsumerChunkedDecoder, ByteBuf, ByteBuf>, WithBinaryChannelInput<BufsConsumerChunkedDecoder> {
	public static final int MAX_CHUNK_LENGTH_DIGITS = 8;
	public static final byte[] CRLF = {13, 10};
	// region exceptions
	public static final ParseException MALFORMED_CHUNK = new ParseException(BufsConsumerChunkedDecoder.class, "Malformed chunk");
	public static final ParseException MALFORMED_CHUNK_LENGTH = new InvalidSizeException(BufsConsumerChunkedDecoder.class, "Malformed chunk length");
	// endregion

	private ByteBufQueue bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerChunkedDecoder() {
	}

	public static BufsConsumerChunkedDecoder create() {
		return new BufsConsumerChunkedDecoder();
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			this.bufs = input.getBufs();
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
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
							if (i == 0 || chunkLength < 0) {
								throw MALFORMED_CHUNK_LENGTH;
							}
							queue.skip(i);
							return chunkLength;
						} else {
							throw MALFORMED_CHUNK_LENGTH;
						}
					}

					if (remainingBytes > MAX_CHUNK_LENGTH_DIGITS) {
						throw MALFORMED_CHUNK_LENGTH;
					}

					return null;
				})
				.acceptEx(Exception.class, this::close)
				.accept(chunkLength -> {
					if (chunkLength != 0) {
						consumeCRLF(chunkLength);
					} else {
						validateLastChunk();
					}
				});
	}

	private void processData(int chunkLength) {
		ByteBuf tempBuf = ByteBufPool.allocate(16 * 1024); // 16Mb - AsyncTcpSocketimpl's default read size
		chunkLength -= bufs.drainTo(tempBuf, chunkLength);
		if (chunkLength != 0) {
			int newChunkLength = chunkLength;
			output.accept(tempBuf)
					.then($ -> input.needMoreData())
					.accept($ -> processData(newChunkLength));
			return;
		}
		input.parse(assertBytes(CRLF))
				.acceptEx(Exception.class, e -> {
					tempBuf.recycle();
					close(MALFORMED_CHUNK);
				})
				.then($ -> output.accept(tempBuf))
				.accept($ -> processLength());
	}

	private void consumeCRLF(int chunkLength) {
		input.parse(
				bufs -> {
					ByteBuf maybeResult = ofCrlfTerminatedBytes().tryParse(bufs);
					if (maybeResult == null) {
						bufs.skip(bufs.remainingBytes() - 1);
					}
					return maybeResult;
				})
				.accept(ByteBuf::recycle)
				.acceptEx(Exception.class, this::close)
				.accept($ -> processData(chunkLength));
	}

	private void validateLastChunk() {
		int remainingBytes = bufs.remainingBytes();
		for (int i = 0; i < remainingBytes - 3; i++) {
			if (bufs.peekByte(i) == CR
					&& bufs.peekByte(i + 1) == LF
					&& bufs.peekByte(i + 2) == CR
					&& bufs.peekByte(i + 3) == LF) {
				bufs.skip(i + 4);

				input.endOfStream()
						.then($ -> output.accept(null))
						.accept($ -> completeProcess());
				return;
			}
		}

		bufs.skip(remainingBytes - 3);

		input.needMoreData()
				.accept($ -> validateLastChunk());
	}

	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		output.close(e);
	}
}
