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

package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

final class GzipProcessorUtils {
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
	private static final int GZIP_HEADER_LENGTH = GZIP_HEADER.length;
	private static final int GZIP_FOOTER_LENGTH = 8;

	private final static int FHCRC = 2;
	private final static int FEXTRA = 4;
	private final static int FNAME = 8;
	private final static int FCOMMENT = 16;

	private GzipProcessorUtils() {}

	static ByteBuf fromGzip(ByteBuf src) throws ParseException {
		assert src.readRemaining() > 0;
		ByteBuf dst = null;
		try {
			processHeader(src);
			Inflater decompressor = wrapForDecompression(src);
			dst = ByteBufPool.allocate(readDecompressedDataSize(src));
			readDecompressedData(decompressor, dst);
			return dst;
		} catch (Exception e) {
			if (dst != null) {
				dst.recycle();
			}
			throw new ParseException("Can't decode gzip", e);
		} finally {
			src.recycle();
		}
	}

	static ByteBuf toGzip(ByteBuf src) {
		assert src.readRemaining() > 0;

		Deflater compressor = wrapForCompression(src);
		int dataLength = src.readRemaining();
		int crc = getCrc(src, dataLength);
		ByteBuf data = ByteBufPool.allocate(GZIP_HEADER_LENGTH + dataLength + GZIP_FOOTER_LENGTH);

		data.put(GZIP_HEADER);
		data = writeCompressedData(compressor, data);
		data.writeInt(Integer.reverseBytes(crc));
		data.writeInt(Integer.reverseBytes(dataLength));

		src.recycle();
		return data;
	}

	private static Inflater wrapForDecompression(ByteBuf buf) {
		Inflater decompressor = new Inflater(true);
		decompressor.setInput(buf.array(), buf.readPosition(), buf.readRemaining());
		return decompressor;
	}

	private static Deflater wrapForCompression(ByteBuf buf) {
		Deflater compressor = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
		compressor.setInput(buf.array(), buf.readPosition(), buf.readRemaining());
		compressor.finish();
		return compressor;
	}

	private static ByteBuf readDecompressedData(Inflater decompressor, ByteBuf buf) throws DataFormatException {
		while (!decompressor.finished()) {
			int count = decompressor.inflate(buf.array(), buf.writePosition(), buf.writeRemaining());
			buf.moveWritePosition(count);
		}
		decompressor.end();
		return buf;
	}

	private static ByteBuf writeCompressedData(Deflater compressor, ByteBuf buf) {
		while (!compressor.finished()) {
			int count = compressor.deflate(buf.array(), buf.writePosition(), buf.writeRemaining());
			buf.moveWritePosition(count);
			buf = ByteBufPool.ensureTailRemaining(buf, buf.readRemaining() * 2);
		}
		compressor.end();
		return buf;
	}

	private static int readDecompressedDataSize(ByteBuf buf) {
		int w = buf.writePosition();
		int r = buf.readPosition();
		// read decompressed data length, represented by little-endian int
		buf.readPosition(w - 4);
		int bigEndianPosition = buf.readInt();
		buf.readPosition(r);
		return Integer.reverseBytes(bigEndianPosition);
	}

	private static int getCrc(ByteBuf buf, int dataLength) {
		CRC32 crc32 = new CRC32();
		crc32.update(buf.array(), buf.readPosition(), dataLength);
		return (int) crc32.getValue();
	}

	private static void processHeader(ByteBuf raw) throws ParseException {
		checkGzipIdentificationConstants(raw);
		checkCompressionMethod(raw);

		byte flag = raw.readByte();
		// skip optional fields
		raw.moveReadPosition(6);
		if ((flag & FEXTRA) > 0) {
			skipExtra(raw);
		}
		if ((flag & FNAME) > 0) {
			skipToTerminatorByte(raw);
		}
		if ((flag & FCOMMENT) > 0) {
			skipToTerminatorByte(raw);
		}
		if ((flag & FHCRC) > 0) {
			raw.moveReadPosition(2);
		}
	}

	private static void checkGzipIdentificationConstants(ByteBuf raw) throws ParseException {
		byte id1 = raw.readByte();
		byte id2 = raw.readByte();
		if (id1 != GZIP_HEADER[0] || id2 != GZIP_HEADER[1]) {
			throw new ParseException("Incorrect identification bytes. Not in GZIP format");
		}
	}

	private static void checkCompressionMethod(ByteBuf raw) throws ParseException {
		byte cm = raw.readByte();
		if (cm != GZIP_HEADER[2]) {
			throw new ParseException("Unsupported compression method. Deflate compression required");
		}
	}

	private static void skipExtra(ByteBuf raw) {
		short m = raw.readShort();
		short reversedM = Short.reverseBytes(m);
		raw.moveReadPosition(reversedM);
	}

	private static void skipToTerminatorByte(ByteBuf raw) {
		while (raw.peek() != 0) {
			raw.moveReadPosition(1);
		}
	}
}
