package io.datakernel.http.stream2;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.processor.AbstractIOAsyncProcess;
import io.datakernel.serial.processor.WithByteBufsInput;
import io.datakernel.serial.processor.WithSerialToSerial;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.Math.min;

public final class BufsConsumerGzipInflater extends AbstractIOAsyncProcess
		implements WithSerialToSerial<BufsConsumerGzipInflater, ByteBuf, ByteBuf>, WithByteBufsInput<BufsConsumerGzipInflater> {
	public static final int DEFAULT_HEADER_FIELD_LENGTH = 1024; //1 Kb
	public static final int DEFAULT_COMPRESSION_RATIO = 20; // Compression ratio of 20 : 1
	public static final int DEFAULT_MAX_BUF_SIZE = 512;
	// region exceptions
	public static final ParseException ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED = new ParseException("Decompressed data size is not equal to input size from GZIP trailer");
	public static final ParseException CRC32_VALUE_DIFFERS = new ParseException("CRC32 value of uncompressed data differs");
	public static final ParseException INCORRECT_ID_HEADER_BYTES = new ParseException("Incorrect identification bytes. Not in GZIP format");
	public static final ParseException UNSUPPORTED_COMPRESSION_METHOD = new ParseException("Unsupported compression method. Deflate compression required");
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

	private final CRC32 crc32 = new CRC32();

	private int maxBufSize = DEFAULT_MAX_BUF_SIZE;
	private int maxHeaderFieldLength = DEFAULT_HEADER_FIELD_LENGTH;
	private int maxCompressionRatio = DEFAULT_COMPRESSION_RATIO;
	private Inflater inflater = new Inflater(true);

	private ByteBufQueue bufs;
	private ByteBufsSupplier input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerGzipInflater() {}

	public static BufsConsumerGzipInflater create() {
		return new BufsConsumerGzipInflater();
	}

	public BufsConsumerGzipInflater withInflater(Inflater inflater){
		checkArgument(inflater != null, "Cannot use null Inflater");
		this.inflater = inflater;
		return this;
	}

	public BufsConsumerGzipInflater withMaxBufSize(int maxBufSize){
		checkArgument(maxBufSize > 0, "Cannot use buf size that is less than 0");
		this.maxBufSize = maxBufSize;
		return this;
	}

	public BufsConsumerGzipInflater withMaxHeaderFieldLength(int maxHeaderFieldLength){
		checkArgument(maxHeaderFieldLength > 0, "Cannot use header field length that is less than 0");
		this.maxHeaderFieldLength = maxHeaderFieldLength;
		return this;
	}

	public BufsConsumerGzipInflater withMaxCompressionratio(int maxCompressionRatio){
		checkArgument(maxCompressionRatio > 0, "Cannot use negative compression raio");
		this.maxCompressionRatio = maxCompressionRatio;
		return this;
	}

	@Override
	public void setInput(ByteBufsSupplier input) {
		checkState(this.input == null, "Input already set");
		this.input = sanitize(input);
		this.bufs = input.bufs;
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		checkState(this.output == null, "Output already set");
		this.output = sanitize(output);
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}


	@Override
	protected void doProcess() {
		processHeader(new byte[]{0});
	}

	private void processHeader(byte[] flag) {
		if (flag[0] == 0) {
			if (!bufs.hasRemainingBytes(10)) {
				input.needMoreData()
						.thenRun(() -> processHeader(flag));
				return;
			}

			byte id1 = bufs.getByte();
			byte id2 = bufs.getByte();
			byte cm = bufs.getByte();

			if (id1 != GZIP_HEADER[0] || id2 != GZIP_HEADER[1]) {
				closeWithError(INCORRECT_ID_HEADER_BYTES);
				return;
			}
			if (cm != GZIP_HEADER[2]) {
				closeWithError(UNSUPPORTED_COMPRESSION_METHOD);
				return;
			}

			flag[0] = bufs.getByte();
			if ((flag[0] & 0b11100000) > 0) {
				closeWithError(MALFORMED_FLAG);
				return;
			}
			bufs.skip(6);
			// unsetting FTEXT bit
			flag[0] &= ~1;
		}

		// trying to skip optional gzip file members if any is present
		try {
			if ((flag[0] & FEXTRA)   != 0 && !skipExtra(flag)) return;
			if ((flag[0] & FNAME)    != 0 && !skipTerminatorByte(flag, FNAME)) return;
			if ((flag[0] & FCOMMENT) != 0 && !skipTerminatorByte(flag, FCOMMENT)) return;
			if ((flag[0] & FHCRC)    != 0 && !skipCRC16(flag)) return;

			processBody();

		} catch (ParseException e) {
			closeWithError(e);
		}
	}

	private void processBody() {
		ByteBufQueue queue = new ByteBufQueue();

		while (bufs.hasRemaining()) {
			ByteBuf src = bufs.peekBuf();
			assert src != null;
			inflater.setInput(src.array(), src.readPosition(), src.readRemaining());
			try {
				inflate(queue);
			} catch (ParseException | DataFormatException e) {
				queue.recycle();
				closeWithError(e);
				return;
			}
			if (inflater.finished()) {
				output.acceptAll(queue.asIterator())
						.thenRun(this::processFooter);
				return;
			}
		}

		output.acceptAll(queue.asIterator())
				.thenCompose($ -> input.needMoreData())
				.thenRun(this::processBody);
	}

	private void processFooter() {
		if (!bufs.hasRemainingBytes(GZIP_FOOTER_SIZE)) {
			input.needMoreData()
					.thenRun(this::processFooter);
			return;
		}
		try {
			validateFooter();
		} catch (ParseException e) {
			closeWithError(e);
			return;
		}

		input.endOfStream()
				.thenCompose($ -> output.accept(null))
				.thenRun(this::completeProcess);
	}

	private void inflate(ByteBufQueue queue) throws ParseException, DataFormatException {
		ByteBuf src = bufs.peekBuf();
		assert src != null;
		while (true) {
			ByteBuf buf = ByteBufPool.allocate(maxBufSize);
			int beforeInflation = inflater.getTotalIn();
			int len = inflater.inflate(buf.array(), 0, buf.writeRemaining());
			buf.moveWritePosition(len);
			src.moveReadPosition(inflater.getTotalIn() - beforeInflation);
			if (len == 0) {
				if (!src.canRead()) {
					bufs.take().recycle();
				}
				buf.recycle();
				return;
			}
			if (inflater.getBytesWritten() / inflater.getBytesRead() > maxCompressionRatio) {
				buf.recycle();
				throw COMPRESSION_RATIO_TOO_LARGE;
			}
			crc32.update(buf.asArray());
			queue.add(buf);
		}
	}

	private void getMoreHeaderData(byte[] flag) {
		input.needMoreData()
				.thenRun(() -> processHeader(flag));
	}

	// region skip header fields
	private boolean skipTerminatorByte(byte[] flag, int part) throws ParseException {
		int remainingBytes = bufs.remainingBytes();
		for (int i = 0; i < min(remainingBytes, maxHeaderFieldLength); i++) {
			if (bufs.peekByte(i) == 0) {
				bufs.skip(i + 1);
				flag[0] -= part;
				return true;
			}
		}
		if (remainingBytes > maxHeaderFieldLength) {
			throw FNAME_FCOMMENT_TOO_LARGE;
		}

		getMoreHeaderData(flag);
		return false;
	}

	private boolean skipExtra(byte[] flag) throws ParseException {
		if (!bufs.hasRemainingBytes(2)) {
			getMoreHeaderData(flag);
			return false;
		}

		// peek short from inputBufs
		short subFieldDataSize = (short) (((bufs.getByte() & 0xFF) << 8)
				| ((bufs.getByte() & 0xFF)));

		short reversedSubFieldDataSize = Short.reverseBytes(subFieldDataSize);
		if (reversedSubFieldDataSize > maxHeaderFieldLength) {
			throw FEXTRA_TOO_LARGE;
		}

		if (!bufs.hasRemainingBytes(reversedSubFieldDataSize)) {
			getMoreHeaderData(flag);
			return false;
		}
		bufs.skip(reversedSubFieldDataSize);
		flag[0] -= FEXTRA;
		return true;
	}

	private boolean skipCRC16(byte[] flag) {
		if (!bufs.hasRemainingBytes(2)) {
			getMoreHeaderData(flag);
			return false;
		}
		bufs.skip(2);
		flag[0] -= FHCRC;
		return true;
	}
	// endregion

	private void validateFooter() throws ParseException {
		if ((int) crc32.getValue() != readNextInt()) {
			throw CRC32_VALUE_DIFFERS;
		}
		if (inflater.getTotalOut() != readNextInt()) {
			throw ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED;
		}
	}

	private int readNextInt() {
		int bigEndianPosition = ((bufs.getByte() & 0xFF) << 24)
				| ((bufs.getByte() & 0xFF) << 16)
				| ((bufs.getByte() & 0xFF) << 8)
				| (bufs.getByte() & 0xFF);
		return Integer.reverseBytes(bigEndianPosition);
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		inflater.end();
		input.closeWithError(e);
		output.closeWithError(e);
	}
}
