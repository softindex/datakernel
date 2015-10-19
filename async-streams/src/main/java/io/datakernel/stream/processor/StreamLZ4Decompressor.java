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
import io.datakernel.stream.StreamDataReceiver;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.stream.processor.StreamLZ4Compressor.*;
import static java.lang.Math.min;

public final class StreamLZ4Decompressor extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> implements StreamLZ4DecompressorMBean {
	private final UpstreamConsumer upstreamConsumer;
	private final DownstreamProducer downstreamProducer;

	private final class UpstreamConsumer extends AbstractUpstreamConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			downstreamProducer.sendEndOfStream();
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return downstreamProducer;
		}
	}

	private final class DownstreamProducer extends AbstractDownstreamProducer implements StreamDataReceiver<ByteBuf> {
		private static final int INITIAL_BUFFER_SIZE = 256;

		private final LZ4FastDecompressor decompressor;
		private final StreamingXXHash32 checksum;

		private final ByteBuf headerBuf = ByteBuf.allocate(HEADER_LENGTH);

		private ByteBuf inputBuf;
		private long inputStreamPosition;

		private long jmxBytesInput;
		private long jmxBytesOutput;
		private int jmxBufsInput;
		private int jmxBufsOutput;

		private DownstreamProducer(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
			this.decompressor = decompressor;
			this.checksum = checksum;
			this.inputBuf = ByteBufPool.allocate(INITIAL_BUFFER_SIZE);
		}

		@Override
		protected void onDownstreamSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			upstreamConsumer.resume();
		}

		@Override
		public void onData(ByteBuf buf) {
			jmxBufsInput++;
			jmxBytesInput += buf.remaining();
			try {
				checkState(!header.finished, "Unexpected byteBuf after LZ4 EOS packet %s : %s", this, buf);
				if (getProducerStatus().isOpen()) {
					consumeInputByteBuffer(buf);
				}
			} catch (Exception e) {
				upstreamConsumer.closeWithError(e);
			} finally {
				buf.recycle();
			}
		}

		private void consumeInputByteBuffer(ByteBuf buf) throws Exception {
			while (buf.hasRemaining()) {
				if (isReadingHeader()) {
					// read message header:
					if (headerBuf.position() == 0 && buf.remaining() >= HEADER_LENGTH) {
						readHeader(header, buf.array(), buf.position());
						buf.advance(HEADER_LENGTH);
						headerBuf.position(HEADER_LENGTH);
					} else {
						buf.drainTo(headerBuf, min(headerBuf.remaining(), buf.remaining()));
						if (isReadingHeader())
							break;
						readHeader(header, headerBuf.array(), 0);
					}
					assert !isReadingHeader();
					inputBuf.position(0);
				}

				if (header.finished) {
					break;
				}

				// read message body:
				assert !isReadingHeader();
				ByteBuf outputBuf;
				if (inputBuf.position() == 0 && buf.remaining() >= header.compressedLen) {
					outputBuf = readBody(decompressor, checksum, header, buf.array(), buf.position());
					buf.advance(header.compressedLen);
				} else {
					inputBuf = ByteBufPool.resize(inputBuf, header.compressedLen);
					buf.drainTo(inputBuf, min(inputBuf.remaining(), buf.remaining()));
					if (inputBuf.hasRemaining())
						break;
					outputBuf = readBody(decompressor, checksum, header, inputBuf.array(), 0);
				}
				inputStreamPosition += HEADER_LENGTH + header.compressedLen;
				jmxBufsOutput++;
				jmxBytesOutput += outputBuf.remaining();
				downstreamDataReceiver.onData(outputBuf);
				headerBuf.position(0);
				assert isReadingHeader();
			}
		}

		private boolean isReadingHeader() {
			return headerBuf.hasRemaining();
		}

		@Override
		protected void doCleanup() {
			if (inputBuf != null) {
				inputBuf.recycle();
				inputBuf = null;
			}
		}
	}

	private final Header header = new Header();

	private final static class Header {
		private int originalLen;
		private int compressedLen;
		private int compressionMethod;
		private int check;
		private boolean finished;
	}

	public StreamLZ4Decompressor(Eventloop eventloop, LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
		super(eventloop);
		this.downstreamProducer = new DownstreamProducer(decompressor, checksum);
		this.upstreamConsumer = new UpstreamConsumer();
	}

	public StreamLZ4Decompressor(Eventloop eventloop) {
		this(eventloop, LZ4Factory.fastestInstance().fastDecompressor(), XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED));
	}

	private static ByteBuf readBody(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum, Header header,
	                                byte[] buf, int off) throws Exception {
		ByteBuf outputBuf = ByteBufPool.allocate(header.originalLen);
		outputBuf.limit(header.originalLen);
		switch (header.compressionMethod) {
			case COMPRESSION_METHOD_RAW:
				System.arraycopy(buf, off, outputBuf.array(), 0, header.originalLen);
				break;
			case COMPRESSION_METHOD_LZ4:
				try {
					int compressedLen2 = decompressor.decompress(buf, off, outputBuf.array(), 0, header.originalLen);
					if (header.compressedLen != compressedLen2) {
						throw new IOException("Stream is corrupted");
					}
				} catch (LZ4Exception e) {
					throw new IOException("Stream is corrupted", e);
				}
				break;
			default:
				throw new AssertionError();
		}
		checksum.reset();
		checksum.update(outputBuf.array(), 0, header.originalLen);
		if (checksum.getValue() != header.check) {
			throw new IOException("Stream is corrupted");
		}
		return outputBuf;
	}

	private static void readHeader(Header header, byte[] buf, int off) throws Exception {
		for (int i = 0; i < MAGIC_LENGTH; ++i) {
			if (buf[off + i] != MAGIC[i]) {
				throw new IOException("Stream is corrupted");
			}
		}
		int token = buf[off + MAGIC_LENGTH] & 0xFF;
		header.compressionMethod = token & 0xF0;
		int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
		if (header.compressionMethod != COMPRESSION_METHOD_RAW && header.compressionMethod != COMPRESSION_METHOD_LZ4) {
			throw new IOException("Stream is corrupted");
		}
		header.compressedLen = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 1);
		header.originalLen = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 5);
		header.check = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 9);
		if (header.originalLen > 1 << compressionLevel
				|| (header.originalLen < 0 || header.compressedLen < 0)
				|| (header.originalLen == 0 && header.compressedLen != 0)
				|| (header.originalLen != 0 && header.compressedLen == 0)
				|| (header.compressionMethod == COMPRESSION_METHOD_RAW && header.originalLen != header.compressedLen)) {
			throw new IOException("Stream is corrupted");
		}
		if (header.originalLen == 0) {
			if (header.check != 0) {
				throw new IOException("Stream is corrupted");
			}
			header.finished = true;
		}
	}

	public long getInputStreamPosition() {
		return downstreamProducer.inputStreamPosition;
	}

	@Override
	public long getBytesInput() {
		return downstreamProducer.jmxBytesInput;
	}

	@Override
	public long getBytesOutput() {
		return downstreamProducer.jmxBytesOutput;
	}

	@Override
	public int getBufsInput() {
		return downstreamProducer.jmxBufsInput;
	}

	@Override
	public int getBufsOutput() {
		return downstreamProducer.jmxBufsOutput;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		return '{' + super.toString() +
				" inBytes:" + downstreamProducer.jmxBytesInput +
				" outBytes:" + downstreamProducer.jmxBytesOutput +
				" inBufs:" + downstreamProducer.jmxBufsInput +
				" outBufs:" + downstreamProducer.jmxBufsOutput +
				'}';
	}

}