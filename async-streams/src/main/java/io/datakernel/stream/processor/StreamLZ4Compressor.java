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

package io.datakernel.stream.processor;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1_Stateless;
import io.datakernel.stream.StreamDataReceiver;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

public final class StreamLZ4Compressor extends AbstractStreamTransformer_1_1_Stateless<ByteBuf, ByteBuf> implements StreamDataReceiver<ByteBuf>, StreamLZ4CompressorMBean {
	static final byte[] MAGIC = new byte[]{'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
	static final int MAGIC_LENGTH = MAGIC.length;

	static final int HEADER_LENGTH =
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

	private long jmxBytesInput;
	private long jmxBytesOutput;
	private int jmxBufs;

	/**
	 * Returns new instance of StreamLZ4Compressor without compression.
	 *
	 * @param eventloop event loop in which compressor will run
	 */
	public static StreamLZ4Compressor rawCompressor(Eventloop eventloop) {
		return new StreamLZ4Compressor(eventloop, null);
	}

	/**
	 * Returns new instance of StreamLZ4Compressor with a {@link LZ4Factory#fastCompressor()}.
	 * for data compression.
	 *
	 * @param eventloop event loop in which compressor will run
	 */
	public static StreamLZ4Compressor fastCompressor(Eventloop eventloop) {
		return new StreamLZ4Compressor(eventloop, LZ4Factory.fastestInstance().fastCompressor());
	}

	/**
	 * Returns new instance of StreamLZ4Compressor with a {@link LZ4Factory#highCompressor()}.
	 * for data compression.
	 *
	 * @param eventloop event loop in which compressor will run
	 */
	public static StreamLZ4Compressor highCompressor(Eventloop eventloop) {
		return new StreamLZ4Compressor(eventloop, LZ4Factory.fastestInstance().highCompressor());
	}

	/**
	 * Returns new instance of StreamLZ4Compressor with a {@link LZ4Factory#highCompressor(int)}
	 * for data compression.
	 *
	 * @param eventloop        event loop in which compressor will run
	 * @param compressionLevel compression level in the same manner as the {@link LZ4Factory#highCompressor(int)}
	 */
	public static StreamLZ4Compressor highCompressor(Eventloop eventloop, int compressionLevel) {
		return new StreamLZ4Compressor(eventloop, LZ4Factory.fastestInstance().highCompressor(compressionLevel));
	}

	/**
	 * Returns new instance of this compressor. Large blocks require more memory at compression
	 * and decompression time but should improve the compression ratio.
	 *
	 * @param eventloop  event loop in which compressor will run
	 * @param compressor compressor which will use; can be {@code null} for transmission without compression
	 */
	private StreamLZ4Compressor(Eventloop eventloop, LZ4Compressor compressor) {
		super(eventloop);
		this.compressor = compressor;
	}

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

	public static ByteBuf compressBlock(LZ4Compressor compressor, StreamingXXHash32 checksum,
	                                    byte[] buffer, int off, int len) {
		int compressionLevel = compressionLevel(len < MIN_BLOCK_SIZE ? MIN_BLOCK_SIZE : len);

		int outputBufMaxSize = HEADER_LENGTH + ((compressor == null) ? len : compressor.maxCompressedLength(len));
		ByteBuf outputBuf = ByteBufPool.allocate(outputBufMaxSize);
		byte[] outputBytes = outputBuf.array();
		System.arraycopy(MAGIC, 0, outputBytes, 0, MAGIC_LENGTH);

		checksum.reset();
		checksum.update(buffer, off, len);
		int check = checksum.getValue();

		int compressedLength = len;
		if (compressor != null) {
			compressedLength = compressor.compress(buffer, off, len, outputBytes, HEADER_LENGTH);
		}

		int compressMethod;
		if (compressor == null || compressedLength >= len) {
			compressMethod = COMPRESSION_METHOD_RAW;
			compressedLength = len;
			System.arraycopy(buffer, off, outputBytes, HEADER_LENGTH, len);
		} else {
			compressMethod = COMPRESSION_METHOD_LZ4;
		}

		outputBytes[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
		writeIntLE(compressedLength, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(len, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(check, outputBytes, MAGIC_LENGTH + 9);
		assert MAGIC_LENGTH + 13 == HEADER_LENGTH;

		outputBuf.limit(HEADER_LENGTH + compressedLength);

		return outputBuf;
	}

	public static ByteBuf createEndOfStreamBlock() {
		int compressionLevel = compressionLevel(MIN_BLOCK_SIZE);

		ByteBuf outputBuf = ByteBufPool.allocate(HEADER_LENGTH);
		byte[] outputBytes = outputBuf.array();
		System.arraycopy(MAGIC, 0, outputBytes, 0, MAGIC_LENGTH);

		outputBytes[MAGIC_LENGTH] = (byte) (COMPRESSION_METHOD_RAW | compressionLevel);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 9);
		assert MAGIC_LENGTH + 13 == HEADER_LENGTH;

		outputBuf.limit(HEADER_LENGTH);
		return outputBuf;
	}

	@Override
	public void onData(ByteBuf buf) {
		if (status >= END_OF_STREAM)
			return;
		try {
			jmxBufs++;
			jmxBytesInput += buf.remaining();

			ByteBuf outputBuffer = compressBlock(compressor, checksum,
					buf.array(), buf.position(), buf.remaining());
			jmxBytesOutput += outputBuffer.remaining();

			send(outputBuffer);

			buf.recycle();
		} catch (Exception e) {
			onInternalError(e);
		}
	}

	@Override
	public void onEndOfStream() {
		send(createEndOfStreamBlock());
		sendEndOfStream();
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	@Override
	public long getBytesInput() {
		return jmxBytesInput;
	}

	@Override
	public long getBytesOutput() {
		return jmxBytesOutput;
	}

	@Override
	public int getBufs() {
		return jmxBufs;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		return '{' + super.toString() +
				" inBytes:" + jmxBytesInput +
				" outBytes:" + jmxBytesOutput +
				" bufs:" + jmxBufs +
				'}';
	}
}
