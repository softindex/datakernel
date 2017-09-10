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
import io.datakernel.stream.*;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.StreamLZ4Compressor.*;
import static java.lang.String.format;

public final class StreamLZ4Decompressor implements StreamTransformer<ByteBuf,ByteBuf> {
	public static final int HEADER_LENGTH = StreamLZ4Compressor.HEADER_LENGTH;

	private final Eventloop eventloop;
	private final LZ4FastDecompressor decompressor;
	private final StreamingXXHash32 checksum;

	private Input input;
	private Output output;

	private Inspector inspector;

	public interface Inspector {
		void onInputBuf(StreamLZ4Decompressor self, ByteBuf buf);

		void onBlock(StreamLZ4Decompressor self, Header header, ByteBuf inputBuf, ByteBuf outputBuf);
	}

	// region creators
	private StreamLZ4Decompressor(Eventloop eventloop, LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
		this.eventloop = eventloop;
		this.decompressor = decompressor;
		this.checksum = checksum;
		recreate();
	}

	private void recreate() {
		this.output = new Output(eventloop, decompressor, checksum);
		this.input = new Input(eventloop);
	}

	@Override
	public StreamConsumer<ByteBuf> getInput() {
		return input;
	}

	@Override
	public StreamProducer<ByteBuf> getOutput() {
		return output;
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
		this.inspector = inspector;
		recreate();
		return this;
	}
	// endregion

	private final class Input extends AbstractStreamConsumer<ByteBuf> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			output.produce();
		}

		@Override
		protected void onError(Exception e) {
			output.closeWithError(e);
		}
	}

	private final class Output extends AbstractStreamProducer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
		private final LZ4FastDecompressor decompressor;
		private final StreamingXXHash32 checksum;

		private final ByteBufQueue queue = ByteBufQueue.create();

		private final ByteBuf headerBuf = ByteBuf.wrapForWriting(new byte[HEADER_LENGTH]);
		private final Header header = new Header();

		private final Inspector inspector = (Inspector) StreamLZ4Decompressor.this.inspector;

		private Output(Eventloop eventloop, LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
			super(eventloop);
			this.decompressor = decompressor;
			this.checksum = checksum;
		}

		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Exception e) {
			input.closeWithError(e);
		}

		@Override
		public void onData(ByteBuf buf) {
			if (inspector != null) inspector.onInputBuf(StreamLZ4Decompressor.this, buf);
			try {
				if (header.finished) {
					throw new ParseException(format("Unexpected byteBuf after LZ4 EOS packet %s : %s", this, buf));
				}
				queue.add(buf);
				output.produce();
			} catch (ParseException e) {
				input.closeWithError(e);
			}
		}

		@Override
		protected void produce() {
			try {
				while (isReceiverReady()) {
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
					send(outputBuf);
					headerBuf.rewind();
				}

				if (isReceiverReady()) {
					input.getProducer().produce(this);
				}

				if (queue.isEmpty() && input.getStatus() == END_OF_STREAM) {
					output.sendEndOfStream();
				}
			} catch (ParseException e) {
				closeWithError(e);
			}
		}

		@Override
		protected void cleanup() {
			queue.clear();
		}
	}

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

	public final static class Header {
		public int originalLen;
		public int compressedLen;
		public int compressionMethod;
		public int check;
		public boolean finished;
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