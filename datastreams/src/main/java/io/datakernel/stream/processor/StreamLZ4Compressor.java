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
import io.datakernel.jmx.ValueStats;
import io.datakernel.stream.*;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import static io.datakernel.stream.StreamStatus.END_OF_STREAM;

public final class StreamLZ4Compressor implements StreamTransformer<ByteBuf, ByteBuf> {
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

	private final Eventloop eventloop;
	private final LZ4Compressor compressor;

	private Input input;
	private Output output;

	public interface Inspector extends AbstractStreamTransformer_1_1.Inspector {
		void onBuf(ByteBuf in, ByteBuf out);
	}

	public static class JmxInspector extends AbstractStreamTransformer_1_1.JmxInspector implements Inspector {
		private static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;

		private final ValueStats bytesIn = ValueStats.create(SMOOTHING_WINDOW);
		private final ValueStats bytesOut = ValueStats.create(SMOOTHING_WINDOW);

		@Override
		public void onBuf(ByteBuf in, ByteBuf out) {
			bytesIn.recordValue(in.readRemaining());
			bytesOut.recordValue(out.readRemaining());
		}
	}

	private final class Input extends AbstractStreamConsumer<ByteBuf> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			output.flushAndClose();
		}

		@Override
		protected void onError(Exception e) {
			output.closeWithError(e);
		}
	}

	private final class Output extends AbstractStreamProducer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
		private final LZ4Compressor compressor;
		private final StreamingXXHash32 checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED);

		private Output(Eventloop eventloop, LZ4Compressor compressor) {
			super(eventloop);
			this.compressor = compressor;
		}

		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void produce() {
			if (input.getStatus() != END_OF_STREAM) {
				input.getProducer().produce(this);
			} else {
				flushAndClose();
			}
		}

		private void flushAndClose() {
			if (isReceiverReady()) {
				send(createEndOfStreamBlock());
				sendEndOfStream();
			}
		}

		@Override
		protected void onError(Exception e) {
			input.closeWithError(e);
		}

		@Override
		public void onData(ByteBuf buf) {
			if (buf.canRead()) {
				ByteBuf outputBuf = compressBlock(compressor, checksum, buf.array(), buf.readPosition(), buf.readRemaining());
				send(outputBuf);
			}
			buf.recycle();
		}
	}

	// region creators
	private StreamLZ4Compressor(Eventloop eventloop, LZ4Compressor compressor) {
		this.eventloop = eventloop;
		this.compressor = compressor;
		rebuild();
	}

	protected void rebuild() {
		this.input = new Input(eventloop);
		this.output = new Output(eventloop, compressor);
	}

	public static StreamLZ4Compressor rawCompressor(Eventloop eventloop) {
		return new StreamLZ4Compressor(eventloop, null);
	}

	public static StreamLZ4Compressor fastCompressor(Eventloop eventloop) {
		return new StreamLZ4Compressor(eventloop, LZ4Factory.fastestInstance().fastCompressor());
	}

	public static StreamLZ4Compressor highCompressor(Eventloop eventloop) {
		return new StreamLZ4Compressor(eventloop, LZ4Factory.fastestInstance().highCompressor());
	}

	public static StreamLZ4Compressor highCompressor(Eventloop eventloop, int compressionLevel) {
		return new StreamLZ4Compressor(eventloop, LZ4Factory.fastestInstance().highCompressor(compressionLevel));
	}

	@Override
	public StreamConsumer<ByteBuf> getInput() {
		return input;
	}

	@Override
	public StreamProducer<ByteBuf> getOutput() {
		return output;
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