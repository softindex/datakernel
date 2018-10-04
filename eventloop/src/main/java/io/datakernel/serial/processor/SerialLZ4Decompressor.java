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

import io.datakernel.async.MaterializedStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import static io.datakernel.serial.processor.SerialLZ4Compressor.*;

public final class SerialLZ4Decompressor extends AbstractIOAsyncProcess
		implements WithSerialToSerial<SerialLZ4Decompressor, ByteBuf, ByteBuf>, WithByteBufsInput<SerialLZ4Decompressor> {
	public static final int HEADER_LENGTH = SerialLZ4Compressor.HEADER_LENGTH;

	private final LZ4FastDecompressor decompressor;
	private final StreamingXXHash32 checksum;

	private ByteBufQueue bufs;
	private ByteBufsSupplier input;
	private SerialConsumer<ByteBuf> output;

	private final Header header = new Header();

	private Inspector inspector;

	public interface Inspector {
		void onBlock(SerialLZ4Decompressor self, Header header, ByteBuf inputBuf, ByteBuf outputBuf);
	}

	// region creators
	private SerialLZ4Decompressor(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
		this.decompressor = decompressor;
		this.checksum = checksum;
	}

	public static SerialLZ4Decompressor create() {
		return create(
				LZ4Factory.fastestInstance().fastDecompressor(),
				XXHashFactory.fastestInstance());
	}

	public static SerialLZ4Decompressor create(LZ4FastDecompressor decompressor, XXHashFactory xxHashFactory) {
		return new SerialLZ4Decompressor(decompressor, xxHashFactory.newStreamingHash32(DEFAULT_SEED));
	}

	public SerialLZ4Decompressor withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	@Override
	public MaterializedStage<Void> setInput(ByteBufsSupplier input) {
		this.input = sanitize(input);
		this.bufs = input.bufs;
		return getResult();
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		this.output = sanitize(output);
	}

	// endregion

	@Override
	protected void doProcess() {
		processHeader();
	}

	public void processHeader() {
		if (!bufs.hasRemainingBytes(HEADER_LENGTH)) {
			input.needMoreData()
					.whenResult($ -> processHeader());
			return;
		}

		try (ByteBuf headerBuf = bufs.takeExactSize(HEADER_LENGTH)) {
			readHeader(header, headerBuf.array(), headerBuf.readPosition());
		} catch (ParseException e) {
			close(e);
			return;
		}

		if (!header.finished) {
			processBody();
			return;
		}

		input.endOfStream()
				.thenCompose($ -> output.accept(null))
				.whenResult($1 -> completeProcess());
	}

	public void processBody() {
		if (!bufs.hasRemainingBytes(header.compressedLen)) {
			input.needMoreData()
					.whenResult($ -> processBody());
			return;
		}

		ByteBuf inputBuf = bufs.takeExactSize(header.compressedLen);
		ByteBuf outputBuf;
		try {
			outputBuf = decompress(decompressor, checksum, header, inputBuf.array(), inputBuf.readPosition());
			if (inspector != null) inspector.onBlock(this, header, inputBuf, outputBuf);
		} catch (ParseException e) {
			close(e);
			return;
		} finally {
			inputBuf.recycle();
		}

		output.accept(outputBuf)
				.whenResult($ -> processHeader());
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.close(e);
		output.close(e);
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

	private static ByteBuf decompress(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum, Header header,
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
				throw new ParseException();
		}
		checksum.reset();
		checksum.update(outputBuf.array(), 0, header.originalLen);
		if (checksum.getValue() != header.check) {
			throw new ParseException("Stream is corrupted");
		}
		return outputBuf;
	}

}
