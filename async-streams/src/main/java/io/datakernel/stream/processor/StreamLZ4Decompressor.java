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
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import static io.datakernel.stream.processor.StreamLZ4Compressor.*;
import static java.lang.Math.min;
import static java.lang.String.format;

public final class StreamLZ4Decompressor extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> implements EventloopJmxMBean {
	private final static class Header {
		private int originalLen;
		private int compressedLen;
		private int compressionMethod;
		private int check;
		private boolean finished;
	}

	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	private final Header header = new Header();

	private final class InputConsumer extends AbstractInputConsumer {
		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.sendEndOfStream();
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return outputProducer;
		}
	}

	private final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<ByteBuf> {
		private static final int INITIAL_BUFFER_SIZE = 256;

		private final LZ4FastDecompressor decompressor;
		private final StreamingXXHash32 checksum;

		private final ByteBuf headerBuf = ByteBuf.wrapForWriting(new byte[HEADER_LENGTH]);

		private ByteBuf inputBuf;
		private long inputStreamPosition;

		private long jmxBytesInput;
		private long jmxBytesOutput;
		private int jmxBufsInput;
		private int jmxBufsOutput;

		private OutputProducer(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
			this.decompressor = decompressor;
			this.checksum = checksum;
			this.inputBuf = ByteBufPool.allocate(INITIAL_BUFFER_SIZE);
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}

		@Override
		public void onData(ByteBuf buf) {
			jmxBufsInput++;
			jmxBytesInput += buf.headRemaining();
			try {
				if (header.finished) {
					throw new ParseException(format("Unexpected byteBuf after LZ4 EOS packet %s : %s", this, buf));
				}
				consumeInputByteBuffer(buf);
			} catch (ParseException e) {
				inputConsumer.closeWithError(e);
			} finally {
				buf.recycle();
			}
		}

		private void consumeInputByteBuffer(ByteBuf buf) throws ParseException {
			while (buf.canRead() && getProducerStatus().isOpen()) {
				if (isReadingHeader()) {
					if (headerBuf.tail() == 0 && buf.headRemaining() >= HEADER_LENGTH) {
						readHeader(header, buf.array(), buf.head());
						buf.moveHead(HEADER_LENGTH);
						headerBuf.tail(HEADER_LENGTH);
					} else {
						buf.drainTo(headerBuf, min(headerBuf.tailRemaining(), buf.headRemaining()));
						if (isReadingHeader()) break;
						readHeader(header, headerBuf.array(), 0);
					}
					assert !isReadingHeader();
				}

				if (header.finished) {
					inputStreamPosition += HEADER_LENGTH; // end-of-stream block size
					break;
				}

				// read message body:
				assert !isReadingHeader();
				ByteBuf outputBuf;
				if (!inputBuf.canRead() && buf.headRemaining() >= header.compressedLen) {
					outputBuf = readBody(decompressor, checksum, header, buf.array(), buf.head());
					buf.moveHead(header.compressedLen);
				} else {
					inputBuf = ByteBufPool.ensureTailRemaining(inputBuf, header.compressedLen);
					int remainingToProcessBytes = header.compressedLen - inputBuf.headRemaining();
					int size = min(remainingToProcessBytes, buf.headRemaining());
					buf.drainTo(inputBuf, size);
					if (inputBuf.headRemaining() < header.compressedLen) break;
					outputBuf = readBody(decompressor, checksum, header, inputBuf.array(), 0);
				}
				inputStreamPosition += HEADER_LENGTH + header.compressedLen;
				jmxBufsOutput++;
				jmxBytesOutput += outputBuf.headRemaining();

				inputBuf.rewind();
				headerBuf.rewind();
				downstreamDataReceiver.onData(outputBuf);
				assert isReadingHeader();
			}
		}

		private boolean isReadingHeader() {
			return headerBuf.canWrite(); // while reading header we need to fill all 21 bytes with data
		}

		@Override
		protected void doCleanup() {
			if (inputBuf != null) {
				inputBuf.recycle();
				inputBuf = null;
			}
		}

		@Override
		public String toString() {
			return '{' + super.toString() +
					" producer:" + inputConsumer.getUpstream() +
					" inBytes:" + jmxBytesInput +
					" outBytes:" + jmxBytesOutput +
					" inBufs:" + jmxBufsInput +
					" outBufs:" + jmxBufsOutput +
					'}';
		}
	}

	// region creators
	private StreamLZ4Decompressor(Eventloop eventloop, LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
		super(eventloop);
		this.outputProducer = new OutputProducer(decompressor, checksum);
		this.inputConsumer = new InputConsumer();
	}

	public static StreamLZ4Decompressor create(Eventloop eventloop, LZ4FastDecompressor decompressor,
	                                           StreamingXXHash32 checksum) {
		return new StreamLZ4Decompressor(eventloop, decompressor, checksum);
	}

	public static StreamLZ4Decompressor create(Eventloop eventloop) {
		return new StreamLZ4Decompressor(eventloop, LZ4Factory.fastestInstance().fastDecompressor(),
				XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED));
	}
	// endregion

	private static ByteBuf readBody(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum, Header header,
	                                byte[] bytes, int off) throws ParseException {
		ByteBuf outputBuf = ByteBufPool.allocate(header.originalLen);
		outputBuf.tail(header.originalLen);
		switch (header.compressionMethod) {
			case COMPRESSION_METHOD_RAW:
				System.arraycopy(bytes, off, outputBuf.array(), 0, header.originalLen);
				break;
			case COMPRESSION_METHOD_LZ4:
				try {
					int compressedLen2 = decompressor.decompress(bytes, off, outputBuf.array(), 0, header.originalLen);
					if (header.compressedLen != compressedLen2) {
						throw new ParseException("Stream is corrupted");
					}
				} catch (LZ4Exception e) {
					throw new ParseException("Stream is corrupted", e);
				}
				break;
			default:
				throw new AssertionError();
		}
		checksum.reset();
		checksum.update(outputBuf.array(), 0, header.originalLen);
		if (checksum.getValue() != header.check) {
			throw new ParseException("Stream is corrupted");
		}
		return outputBuf;
	}

	private static void readHeader(Header header, byte[] buf, int off) throws ParseException {
		for (int i = 0; i < MAGIC_LENGTH; ++i) {
			if (buf[off + i] != MAGIC[i]) {
				throw new ParseException("Stream is corrupted");
			}
		}
		int token = buf[off + MAGIC_LENGTH] & 0xFF;
		header.compressionMethod = token & 0xF0;
		int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
		if (header.compressionMethod != COMPRESSION_METHOD_RAW && header.compressionMethod != COMPRESSION_METHOD_LZ4) {
			throw new ParseException("Stream is corrupted");
		}
		header.compressedLen = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 1);
		header.originalLen = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 5);
		header.check = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 9);
		if (header.originalLen > 1 << compressionLevel
				|| (header.originalLen < 0 || header.compressedLen < 0)
				|| (header.originalLen == 0 && header.compressedLen != 0)
				|| (header.originalLen != 0 && header.compressedLen == 0)
				|| (header.compressionMethod == COMPRESSION_METHOD_RAW && header.originalLen != header.compressedLen)) {
			throw new ParseException("Stream is corrupted");
		}
		if (header.originalLen == 0) {
			if (header.check != 0) {
				throw new ParseException("Stream is corrupted");
			}
			header.finished = true;
		}
	}

	public long getInputStreamPosition() {
		return outputProducer.inputStreamPosition;
	}

	// jmx
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public long getBytesInput() {
		return outputProducer.jmxBytesInput;
	}

	@JmxAttribute
	public long getBytesOutput() {
		return outputProducer.jmxBytesOutput;
	}

	@JmxAttribute
	public int getBufsInput() {
		return outputProducer.jmxBufsInput;
	}

	@JmxAttribute
	public int getBufsOutput() {
		return outputProducer.jmxBufsOutput;
	}

	// miscellaneous
	@Override
	public String toString() {
		return '{' + super.toString() +
				" inBytes:" + outputProducer.jmxBytesInput +
				" outBytes:" + outputProducer.jmxBytesOutput +
				" inBufs:" + outputProducer.jmxBufsInput +
				" outBufs:" + outputProducer.jmxBufsOutput +
				'}';
	}
}