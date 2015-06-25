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
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.AbstractStreamTransformer_1_1_Stateless;
import io.datakernel.stream.StreamDataReceiver;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * It is realization LZ4 is a lossless data compression algorithm that is focused on compression
 * and decompression speed. It used for storing data in external memory. It is a {@link AbstractStreamTransformer_1_1}
 * which receives ByteBufs and streams compression ByteBufs  to the destination .
 */
public final class StreamLZ4Compressor extends AbstractStreamTransformer_1_1_Stateless<ByteBuf, ByteBuf> implements StreamDataReceiver<ByteBuf>, StreamLZ4CompressorMBean {
	static final byte[] MAGIC = new byte[]{'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
	static final int MAGIC_LENGTH = MAGIC.length;

	static final int HEADER_LENGTH =
			MAGIC_LENGTH // magic bytes
					+ 1          // token
					+ 4          // compressed length
					+ 4          // decompressed length
					+ 4;         // checksum

	static final int COMPRESSION_LEVEL_BASE = 10;
	static final int MIN_BLOCK_SIZE = 64;
	static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

	static final int COMPRESSION_METHOD_RAW = 0x10;
	static final int COMPRESSION_METHOD_LZ4 = 0x20;

	public static final int DEFAULT_SEED = 0x9747b28c;

	private final ByteBufPool pool;

	private final int writeThroughSize;
	private final int blockSize;
	//	private final int compressionLevel;
	private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
	private final StreamingXXHash32 checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED);

	private ByteBuf inputBuf = ByteBuf.allocate(1);

	private long sentBytes;

	private long jmxBytesInput;
	private int jmxBufsInput;
	private int jmxBufsOutput;

	/**
	 * Returns new instance of this compressor. Large blocks require more memory at compression
	 * and decompression time but should improve the compression ratio.
	 *
	 * @param eventloop event loop in which compressor will run
	 * @param blockSize the maximum number of bytes to try to compress at once
	 */
	public StreamLZ4Compressor(Eventloop eventloop, int blockSize, int writeThroughSize) {
		super(eventloop);
		checkArgument(blockSize >= MIN_BLOCK_SIZE, "blockSize must be >= %s, got %s", MIN_BLOCK_SIZE, blockSize);
		checkArgument(blockSize <= MAX_BLOCK_SIZE, "blockSize must be <= %s, got %s", MAX_BLOCK_SIZE, blockSize);
		checkArgument(writeThroughSize >= 0 && writeThroughSize <= blockSize,
				"writeThroughSize must be positive value and it must be less than blockSize, got %s", writeThroughSize);

		this.writeThroughSize = writeThroughSize;
		this.blockSize = blockSize;
		this.pool = eventloop.getByteBufferPool();
//		this.compressionLevel = compressionLevel(blockSize);
	}

	/**
	 * Returns new instance of this compressor. Large blocks require more memory at compression
	 * and decompression time but should improve the compression ratio.
	 *
	 * @param eventloop event loop in which compressor will run
	 * @param blockSize the maximum number of bytes to try to compress at once
	 */
	public StreamLZ4Compressor(Eventloop eventloop, int blockSize) {
		this(eventloop, blockSize, blockSize);
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
		buf[off++] = (byte) (i >>> 24);
	}

	public static ByteBuf compressBlock(LZ4Compressor compressor, StreamingXXHash32 checksum, ByteBufPool pool,
	                                    ByteBuffer buf, boolean endOfStreamBlock) {
		return compressBlock(compressor, checksum, pool,
				buf.array(), buf.arrayOffset() + buf.position(), buf.remaining(), endOfStreamBlock);
	}

	public static ByteBuf compressBlock(LZ4Compressor compressor, StreamingXXHash32 checksum, ByteBufPool pool,
	                                    byte[] buffer, int off, int len, boolean endOfStreamBlock) {
		if (len == 0 && !endOfStreamBlock) {
			return pool.allocate(0);
		}

		int compressionLevel = compressionLevel(len < MIN_BLOCK_SIZE ? MIN_BLOCK_SIZE : len);

		int outputBufMaxSize = HEADER_LENGTH + compressor.maxCompressedLength(len) + (endOfStreamBlock ? HEADER_LENGTH : 0);
		ByteBuf outputBuf = pool.allocate(outputBufMaxSize);
		byte[] outputBytes = outputBuf.array();
		int compressedBlockLength = 0;

		if (len != 0) {
			checksum.reset();
			checksum.update(buffer, off, len);
			int check = checksum.getValue();

			System.arraycopy(MAGIC, 0, outputBytes, 0, MAGIC_LENGTH);
			int compressedLength = compressor.compress(buffer, off, len, outputBytes, HEADER_LENGTH);
			int compressMethod;
			if (compressedLength >= len) {
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

			compressedBlockLength = HEADER_LENGTH + compressedLength;
		}

		if (endOfStreamBlock) {
			System.arraycopy(MAGIC, 0, outputBytes, compressedBlockLength, MAGIC_LENGTH);
			outputBytes[compressedBlockLength + MAGIC_LENGTH] = (byte) (COMPRESSION_METHOD_RAW | compressionLevel);
			writeIntLE(0, outputBytes, compressedBlockLength + MAGIC_LENGTH + 1);
			writeIntLE(0, outputBytes, compressedBlockLength + MAGIC_LENGTH + 5);
			writeIntLE(0, outputBytes, compressedBlockLength + MAGIC_LENGTH + 9);
		}

		outputBuf.limit(compressedBlockLength + (endOfStreamBlock ? HEADER_LENGTH : 0));

		return outputBuf;
	}

	private void flushBufferedData(byte[] buffer, int off, int len, boolean endOfStreamBlock) {
		ByteBuf outputBuffer = compressBlock(compressor, checksum, pool, buffer, off, len, endOfStreamBlock);
		jmxBufsOutput++;
		int size = outputBuffer.remaining();
		if (size != 0) {
			if (status <= SUSPENDED) {
				downstreamDataReceiver.onData(outputBuffer);
			}
			sentBytes += size;
		}
	}

	@Override
	public void onData(ByteBuf buf) {
		if (status >= END_OF_STREAM)
			return;
		try {
			jmxBufsInput++;
			jmxBytesInput += inputBuf.remaining();
			while (inputBuf.position() + buf.remaining() >= blockSize) {
				if (inputBuf.position() == 0) {
					flushBufferedData(buf.array(), buf.position(), blockSize, false);
					buf.advance(blockSize);
				} else {
					inputBuf = pool.resize(inputBuf, blockSize);
					buf.drainTo(inputBuf, inputBuf.remaining());
					flushBufferedData(inputBuf.array(), 0, blockSize, false);
					inputBuf.position(0);
				}
			}

			if (inputBuf.position() == 0 && buf.remaining() >= writeThroughSize) {
				flushBufferedData(buf.array(), buf.position(), buf.remaining(), false);
			} else {
				inputBuf = pool.resize(inputBuf, blockSize);
				buf.drainTo(inputBuf, buf.remaining());
			}

			buf.recycle();
		} catch (Exception e) {
			onInternalError(e);
		}
	}

	@Override
	public void onEndOfStream() {
		flushBufferedData(inputBuf.array(), 0, inputBuf.position(), true);
		sendEndOfStream();
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	public long getSentBytes() {
		return sentBytes;
	}

	@Override
	public long getBytesInput() {
		return jmxBytesInput;
	}

	@Override
	public long getBytesOutput() {
		return sentBytes;
	}

	@Override
	public int getBufsInput() {
		return jmxBufsInput;
	}

	@Override
	public int getBufsOutput() {
		return jmxBufsOutput;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		return '{' + super.toString() +
				" inBytes:" + jmxBytesInput +
				" outBytes:" + sentBytes +
				" inBufs:" + jmxBufsInput +
				" outBufs:" + jmxBufsOutput +
				'}';
	}
}
