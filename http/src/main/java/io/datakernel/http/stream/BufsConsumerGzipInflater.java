package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class BufsConsumerGzipInflater implements BufsConsumer {
	public static final int DEFAULT_HEADER_FIELD_LENGTH = 1024; //1 Kb
	public static final int DEFAULT_COMPRESSION_RATIO = 20; // Compression ratio of 20 : 1
	public static final int DEFAULT_MAX_BUF_SIZE = 512;
	// region exceptions
	public static final ParseException COMPRESSED_DATA_WAS_NOT_READ_FULLY = new ParseException("Compressed data was not read fully");
	public static final ParseException ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED = new ParseException("Decompressed data size is not equal to input size from GZIP trailer");
	public static final ParseException CRC32_VALUE_DIFFERS = new ParseException("CRC32 value of uncompressed data differs");
	public static final ParseException INCORRECT_ID_HEADER_BYTES = new ParseException("Incorrect identification bytes. Not in GZIP format");
	public static final ParseException UNSUPPORTED_COMPRESSION_METHOD = new ParseException("Unsupported compression method. Deflate compression required");
	public static final ParseException NOT_ENOUGH_DATA_TO_UNZIP = new ParseException("Not enough data to unzip");
	public static final ParseException FEXTRA_TOO_LARGE = new ParseException("FEXTRA part of a header is larger than maximum allowed length");
	public static final ParseException FNAME_FCOMMENT_TOO_LARGE = new ParseException("FNAME or FEXTRA header is larger than maximum allowed length");
	public static final ParseException COMPRESSION_RATIO_TOO_LARGE = new ParseException("Compression ratio of gzip exceeds maximum allowed ratio");
	public static final ParseException MALFORMED_FLAG = new ParseException("Flag byte of a header is malformed. Reserved bits are set");
	// endregion
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED};
	private static final int GZIP_FOOTER_SIZE = 8;
	private static final int FHCRC = 2;
	private static final int FEXTRA = 4;
	private static final int FNAME = 8;
	private static final int FCOMMENT = 16;
	private static final byte HEADER = 0b000_00000;
	private static final byte BODY = 0b001_00000;
	private static final byte FOOTER = 0b010_00000;

	private final BufsConsumer next;
	private final Inflater inflater;
	private final int maxBufSize;
	private final ByteBufQueue outputBufs = new ByteBufQueue();
	private final CRC32 crc32 = new CRC32();
	private final int maxHeaderFieldLength;
	private final int maxCompressionRatio;

	private int currentlySkipped;
	private byte reading = HEADER;

	// region creators
	public BufsConsumerGzipInflater(BufsConsumer next, Inflater inflater, int maxBufSize, int maxHeaderFieldLength, int maxCompressionRatio) {
		this.next = next;
		this.inflater = inflater;
		this.maxBufSize = maxBufSize;
		this.maxHeaderFieldLength = maxHeaderFieldLength;
		this.maxCompressionRatio = maxCompressionRatio;
	}

	public BufsConsumerGzipInflater(BufsConsumer next, Inflater inflater, int maxBufSize) {
		this.next = next;
		this.inflater = inflater;
		this.maxBufSize = maxBufSize;
		this.maxHeaderFieldLength = DEFAULT_HEADER_FIELD_LENGTH;
		this.maxCompressionRatio = DEFAULT_COMPRESSION_RATIO;
	}

	public BufsConsumerGzipInflater(BufsConsumer next) {
		this.next = next;
		this.inflater = new Inflater(true);
		this.maxBufSize = DEFAULT_MAX_BUF_SIZE;
		this.maxHeaderFieldLength = DEFAULT_HEADER_FIELD_LENGTH;
		this.maxCompressionRatio = DEFAULT_COMPRESSION_RATIO;
	}
	// endregion

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		try {
			int originalOutputBufSize = outputBufs.remainingBytes();
			while (!inputBufs.isEmpty()) {
				if (reading < BODY) {
					if (!processHeader(inputBufs)) {
						return Stage.of(false);
					}
					reading = BODY;
					continue;
				}
				if (reading == BODY) {
					ByteBuf src = inputBufs.peekBuf();
					assert src != null;
					if (inflater.needsInput()) {
						if (!src.canRead()) {
							inputBufs.take().recycle();
							continue;
						}
						inflater.setInput(src.array(), src.readPosition(), src.readRemaining());
					}

					for (; ; ) {
						if (!inflater.finished()) {
							ByteBuf buf = ByteBufPool.allocate(maxBufSize);
							int n;
							try {
								int beforeInflation = inflater.getTotalIn();
								n = inflater.inflate(buf.array(), 0, buf.writeRemaining());
								buf.moveWritePosition(n);
								src.moveReadPosition(inflater.getTotalIn() - beforeInflation);
							} catch (DataFormatException e) {
								return Stage.ofException(e);
							}

							if (n == 0) {
								if (!src.canRead()) {
									inputBufs.take().recycle();
								}
								buf.recycle();
								break;
							}
							writeInflatedData(buf);
						} else {
							reading = FOOTER;
							break;
						}
					}
				}
				if (reading == FOOTER) {
					if (!inputBufs.hasRemainingBytes(8)) {
						break;
					}
					validateFooter(inputBufs);
					return next.push(outputBufs, true)
							.whenException(this::closeWithError);
				}
			}

			// End of stream reached when data is not fully decoded
			if (endOfStream) {
				closeWithError(NOT_ENOUGH_DATA_TO_UNZIP);
				return Stage.ofException(NOT_ENOUGH_DATA_TO_UNZIP);
			}

			if (originalOutputBufSize == outputBufs.remainingBytes()) {
				return Stage.of(false);
			}

			return next.push(outputBufs, false)
					.whenException(this::closeWithError);

		} catch (ParseException e) {
			closeWithError(e);
			return Stage.ofException(e);
		}
	}

	private boolean processHeader(ByteBufQueue inputBufs) throws ParseException {
		if (reading == HEADER) {
			if (!inputBufs.hasRemainingBytes(10)) {
				return false;
			}

			byte id1 = inputBufs.getByte();
			byte id2 = inputBufs.getByte();
			byte cm = inputBufs.getByte();

			if (id1 != GZIP_HEADER[0] || id2 != GZIP_HEADER[1]) {
				throw INCORRECT_ID_HEADER_BYTES;
			}
			if (cm != GZIP_HEADER[2]) {
				throw UNSUPPORTED_COMPRESSION_METHOD;
			}

			byte flg = inputBufs.getByte();
			if ((flg & 0b11100000) > 0) {
				throw MALFORMED_FLAG;
			}
			inputBufs.skip(6);
			reading += flg;
			// unsetting FTEXT bit
			reading &= ~1;
		}

		if ((reading & FEXTRA) > 0) {
			if (!skipExtra(inputBufs)) {
				return false;
			}
			reading -= FEXTRA;
		}

		if ((reading & FNAME) > 0) {
			if (!skipTerminatorByte(inputBufs)) {
				return false;
			}
			reading -= FNAME;
		}

		if ((reading & FCOMMENT) > 0) {
			if (!skipTerminatorByte(inputBufs)) {
				return false;
			}
			reading -= FCOMMENT;
		}

		if ((reading & FHCRC) > 0) {
			if (!inputBufs.hasRemainingBytes(2)) {
				return false;
			}
			inputBufs.skip(2);
			reading -= FHCRC;
		}

		return true;
	}

	// region skip header fields
	private boolean skipTerminatorByte(ByteBufQueue inputBufs) throws ParseException {
		while (inputBufs.hasRemaining()) {
			if (inputBufs.getByte() == 0) {
				currentlySkipped = 0;
				return true;
			}
			if (++currentlySkipped > maxHeaderFieldLength) {
				throw FNAME_FCOMMENT_TOO_LARGE;
			}
		}
		return false;
	}

	private boolean skipExtra(ByteBufQueue inputBufs) throws ParseException {
		if (!inputBufs.hasRemainingBytes(2)) {
			return false;
		}

		// peek short from inputBufs
		short subFieldDataSize = (short) (((inputBufs.getByte() & 0xFF) << 8)
				| ((inputBufs.getByte() & 0xFF)));

		short reversedSubFieldDataSize = Short.reverseBytes(subFieldDataSize);
		if (reversedSubFieldDataSize > maxHeaderFieldLength) {
			throw FEXTRA_TOO_LARGE;
		}

		if (!inputBufs.hasRemainingBytes(reversedSubFieldDataSize)) {
			return false;
		}
		inputBufs.skip(reversedSubFieldDataSize);
		return true;
	}
	// endregion

	private void writeInflatedData(ByteBuf buf) throws ParseException {
		if (inflater.getBytesWritten() / inflater.getBytesRead() > maxCompressionRatio) {
			buf.recycle();
			throw COMPRESSION_RATIO_TOO_LARGE;
		}
		crc32.update(buf.asArray());
		outputBufs.add(buf);
	}

	private void validateFooter(ByteBufQueue inputBufs) throws ParseException {
		if (inputBufs.remainingBytes() > GZIP_FOOTER_SIZE) {
			throw COMPRESSED_DATA_WAS_NOT_READ_FULLY;
		}
		if ((int) crc32.getValue() != readNextInt(inputBufs)) {
			throw CRC32_VALUE_DIFFERS;
		}
		if (inflater.getTotalOut() != readNextInt(inputBufs)) {
			throw ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED;
		}
	}

	private int readNextInt(ByteBufQueue inputQueue) {
		ByteBuf buf = inputQueue.takeExactSize(4);
		int bigEndianPosition = buf.readInt();
		buf.recycle();
		return Integer.reverseBytes(bigEndianPosition);
	}

	@Override
	public void closeWithError(Throwable e) {
		inflater.end();
		outputBufs.recycle();
		next.closeWithError(e);
	}

}
