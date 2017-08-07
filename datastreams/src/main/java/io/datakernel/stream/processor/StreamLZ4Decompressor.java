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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.ValueStats;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import static io.datakernel.stream.processor.StreamLZ4Compressor.*;
import static java.lang.String.format;

public final class StreamLZ4Decompressor extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> {
	public static final int HEADER_LENGTH = StreamLZ4Compressor.HEADER_LENGTH;

	public final static class Header {
		public int originalLen;
		public int compressedLen;
		public int compressionMethod;
		public int check;
		public boolean finished;
	}

	private final LZ4FastDecompressor decompressor;
	private final StreamingXXHash32 checksum;

	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;

	private final Header header = new Header();

	public interface Inspector extends AbstractStreamTransformer_1_1.Inspector {
		void onInputBuf(StreamLZ4Decompressor self, ByteBuf buf);

		void onBlock(StreamLZ4Decompressor self, Header header, ByteBuf inputBuf, ByteBuf outputBuf);
	}

	public static class JmxInspector extends AbstractStreamTransformer_1_1.JmxInspector implements Inspector {
		private static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;

		private final ValueStats bytesIn = ValueStats.create(SMOOTHING_WINDOW);
		private final ValueStats bytesOut = ValueStats.create(SMOOTHING_WINDOW);

		@Override
		public void onInputBuf(StreamLZ4Decompressor self, ByteBuf buf) {
			bytesIn.recordValue(buf.readRemaining());
		}

		@Override
		public void onBlock(StreamLZ4Decompressor self, Header header, ByteBuf inputBuf, ByteBuf outputBuf) {
			bytesOut.recordValue(outputBuf.readRemaining());
		}
	}

	private final class InputConsumer extends AbstractInputConsumer {
		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.produce();
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return outputProducer;
		}
	}

	private final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<ByteBuf> {
		private final LZ4FastDecompressor decompressor;
		private final StreamingXXHash32 checksum;

		private final ByteBufQueue queue = ByteBufQueue.create();

		private final ByteBuf headerBuf = ByteBuf.wrapForWriting(new byte[HEADER_LENGTH]);

		private final Inspector inspector = (Inspector) StreamLZ4Decompressor.this.inspector;

		private OutputProducer(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
			this.decompressor = decompressor;
			this.checksum = checksum;
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			resumeProduce();
		}

		@Override
		public void onData(ByteBuf buf) {
			if (inspector != null) inspector.onInputBuf(StreamLZ4Decompressor.this, buf);
			try {
				if (header.finished) {
					throw new ParseException(format("Unexpected byteBuf after LZ4 EOS packet %s : %s", this, buf));
				}
				queue.add(buf);
				outputProducer.produce();
			} catch (ParseException e) {
				inputConsumer.closeWithError(e);
			}
		}

		@Override
		protected void doProduce() {
			try {
				while (isStatusReady()) {
					if (!queue.hasRemainingBytes(headerBuf.writeRemaining()))
						break;

					if (headerBuf.canWrite()) {
						queue.drainTo(headerBuf);
						readHeader(header, headerBuf.array(), headerBuf.readPosition());
					}

					if (header.finished) {
						if (!queue.isEmpty()) {
							throw new ParseException(format("Unexpected byteBuf after LZ4 EOS packet %s : %s", this, queue));
						}
						if (inspector != null) inspector.onBlock(StreamLZ4Decompressor.this, header, ByteBuf.empty(), ByteBuf.empty());
						break;
					}

					if (!queue.hasRemainingBytes(header.compressedLen))
						break;

					ByteBuf inputBuf = queue.takeExactSize(header.compressedLen);
					ByteBuf outputBuf = readBody(decompressor, checksum, header, inputBuf.array(), inputBuf.readPosition());
					if (inspector != null) inspector.onBlock(StreamLZ4Decompressor.this, header, inputBuf, outputBuf);
					inputBuf.recycle();
					downstreamDataReceiver.onData(outputBuf);
					headerBuf.rewind();
				}

				if (isStatusReady()) {
					inputConsumer.resume();
				}

				if (queue.isEmpty() && inputConsumer.getConsumerStatus().isClosed()) {
					outputProducer.sendEndOfStream();
				}

			} catch (ParseException e) {
				closeWithError(e);
			}
		}

		@Override
		protected void doCleanup() {
			queue.clear();
		}
	}

	// region creators
	private StreamLZ4Decompressor(Eventloop eventloop, LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
		super(eventloop);
		this.decompressor = decompressor;
		this.checksum = checksum;
		recreate();
	}

	private void recreate() {
		this.outputProducer = new OutputProducer(decompressor, checksum);
		this.inputConsumer = new InputConsumer();
	}

	@Override
	protected AbstractInputConsumer getInputImpl() {
		return inputConsumer;
	}

	@Override
	protected AbstractOutputProducer getOutputImpl() {
		return outputProducer;
	}

	public static StreamLZ4Decompressor create(Eventloop eventloop, LZ4FastDecompressor decompressor,
	                                           StreamingXXHash32 checksum) {
		return new StreamLZ4Decompressor(eventloop, decompressor, checksum);
	}

	public static StreamLZ4Decompressor create(Eventloop eventloop) {
		return new StreamLZ4Decompressor(eventloop, LZ4Factory.fastestInstance().fastDecompressor(),
				XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED));
	}

	public StreamLZ4Decompressor withInspector(Inspector inspector) {
		super.inspector = inspector;
		recreate();
		return this;
	}
	// endregion

	private static ByteBuf readBody(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum, Header header,
	                                byte[] bytes, int off) throws ParseException {
		ByteBuf outputBuf = ByteBufPool.allocate(header.originalLen);
		outputBuf.writePosition(header.originalLen);
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

}