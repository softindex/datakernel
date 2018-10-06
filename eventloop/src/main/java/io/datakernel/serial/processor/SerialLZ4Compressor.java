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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.jmx.ValueStats;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.time.Duration;

public final class SerialLZ4Compressor extends AbstractIOAsyncProcess
		implements WithSerialToSerial<SerialLZ4Compressor, ByteBuf, ByteBuf> {
	static final byte[] MAGIC = new byte[]{'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
	static final int MAGIC_LENGTH = MAGIC.length;

	public static final int HEADER_LENGTH =
			MAGIC_LENGTH    // magic bytes
					+ 1     // token
					+ 4     // compressed length
					+ 4     // decompressed length
					+ 4;    // checksum

	static final int COMPRESSION_LEVEL_BASE = 10;

	static final int COMPRESSION_METHOD_RAW = 0x10;
	static final int COMPRESSION_METHOD_LZ4 = 0x20;

	static final int DEFAULT_SEED = 0x9747b28c;

	private static final int MIN_BLOCK_SIZE = 64;

	private final LZ4Compressor compressor;
	private final StreamingXXHash32 checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED);

	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	public interface Inspector {
		void onBuf(ByteBuf in, ByteBuf out);
	}

	public static class JmxInspector implements Inspector {
		public static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final ValueStats bytesIn = ValueStats.create(SMOOTHING_WINDOW);
		private final ValueStats bytesOut = ValueStats.create(SMOOTHING_WINDOW);

		@Override
		public void onBuf(ByteBuf in, ByteBuf out) {
			bytesIn.recordValue(in.readRemaining());
			bytesOut.recordValue(out.readRemaining());
		}
	}

	// region creators
	private SerialLZ4Compressor(LZ4Compressor compressor) {
		this.compressor = compressor;
	}

	public static SerialLZ4Compressor create(LZ4Compressor compressor) {
		return new SerialLZ4Compressor(compressor);
	}

	public static SerialLZ4Compressor create(int compressionLevel) {
		return compressionLevel == 0 ? createFastCompressor() : createHighCompressor(compressionLevel);
	}

	public static SerialLZ4Compressor createFastCompressor() {
		return new SerialLZ4Compressor(LZ4Factory.fastestInstance().fastCompressor());
	}

	public static SerialLZ4Compressor createHighCompressor() {
		return new SerialLZ4Compressor(LZ4Factory.fastestInstance().highCompressor());
	}

	public static SerialLZ4Compressor createHighCompressor(int compressionLevel) {
		return new SerialLZ4Compressor(LZ4Factory.fastestInstance().highCompressor(compressionLevel));
	}

	@Override
	public SerialInput<ByteBuf> getInput() {
		return input -> {
			this.input = sanitize(input);
			if (this.input != null && this.output != null) start();
			return getResult();
		};
	}

	@Override
	public SerialOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = sanitize(output);
			if (this.input != null && this.output != null) start();
		};
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (buf != null) {
						ByteBuf outputBuf = compressBlock(compressor, checksum, buf.array(), buf.readPosition(), buf.readRemaining());
						buf.recycle();
						output.accept(outputBuf)
								.whenResult($ -> doProcess());
					} else {
						output.accept(createEndOfStreamBlock(), null)
								.whenResult($ -> completeProcess());
					}
				});
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.close(e);
		output.close(e);
	}

	// endregion

	private static int compressionLevel(int blockSize) {
		int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
		assert (1 << compressionLevel) >= blockSize;
		assert blockSize * 2 > (1 << compressionLevel);
		compressionLevel = Math.max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
		assert compressionLevel >= 0 && compressionLevel <= 0x0F;
		return compressionLevel;
	}

	private static void writeIntLE(int i, byte[] buf, int off) {
		buf[off++] = (byte) i;
		buf[off++] = (byte) (i >>> 8);
		buf[off++] = (byte) (i >>> 16);
		buf[off] = (byte) (i >>> 24);
	}

	private static ByteBuf compressBlock(LZ4Compressor compressor, StreamingXXHash32 checksum, byte[] bytes, int off, int len) {
		assert len != 0;

		int compressionLevel = compressionLevel(len < MIN_BLOCK_SIZE ? MIN_BLOCK_SIZE : len);

		int outputBufMaxSize = HEADER_LENGTH + ((compressor == null) ? len : compressor.maxCompressedLength(len));
		ByteBuf outputBuf = ByteBufPool.allocate(outputBufMaxSize);
		outputBuf.put(MAGIC);

		byte[] outputBytes = outputBuf.array();

		checksum.reset();
		checksum.update(bytes, off, len);
		int check = checksum.getValue();

		int compressedLength = len;
		if (compressor != null) {
			compressedLength = compressor.compress(bytes, off, len, outputBytes, HEADER_LENGTH);
		}

		int compressMethod;
		if (compressor == null || compressedLength >= len) {
			compressMethod = COMPRESSION_METHOD_RAW;
			compressedLength = len;
			System.arraycopy(bytes, off, outputBytes, HEADER_LENGTH, len);
		} else {
			compressMethod = COMPRESSION_METHOD_LZ4;
		}

		outputBytes[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
		writeIntLE(compressedLength, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(len, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(check, outputBytes, MAGIC_LENGTH + 9);
		assert MAGIC_LENGTH + 13 == HEADER_LENGTH;

		outputBuf.writePosition(HEADER_LENGTH + compressedLength);

		return outputBuf;
	}

	private static ByteBuf createEndOfStreamBlock() {
		int compressionLevel = compressionLevel(MIN_BLOCK_SIZE);

		ByteBuf outputBuf = ByteBufPool.allocate(HEADER_LENGTH);
		byte[] outputBytes = outputBuf.array();
		System.arraycopy(MAGIC, 0, outputBytes, 0, MAGIC_LENGTH);

		outputBytes[MAGIC_LENGTH] = (byte) (COMPRESSION_METHOD_RAW | compressionLevel);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 9);

		outputBuf.writePosition(HEADER_LENGTH);
		return outputBuf;
	}

}
