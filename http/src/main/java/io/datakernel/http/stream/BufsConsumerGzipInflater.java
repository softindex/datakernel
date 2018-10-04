package io.datakernel.http.stream;

import io.datakernel.async.MaterializedStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.processor.AbstractIOAsyncProcess;
import io.datakernel.serial.processor.WithByteBufsInput;
import io.datakernel.serial.processor.WithSerialToSerial;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static io.datakernel.serial.ByteBufsParser.ofFixedSize;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.Integer.reverseBytes;
import static java.lang.Math.max;
import static java.lang.Short.reverseBytes;

public final class BufsConsumerGzipInflater extends AbstractIOAsyncProcess
		implements WithSerialToSerial<BufsConsumerGzipInflater, ByteBuf, ByteBuf>, WithByteBufsInput<BufsConsumerGzipInflater> {
	public static final int MAX_HEADER_FIELD_LENGTH = 4096; //4 Kb
	public static final int DEFAULT_BUF_SIZE = 512;
	// region exceptions
	public static final ParseException ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED = new ParseException("Decompressed data size is not equal to input size from GZIP trailer");
	public static final ParseException CRC32_VALUE_DIFFERS = new ParseException("CRC32 value of uncompressed data differs");
	public static final ParseException INCORRECT_ID_HEADER_BYTES = new ParseException("Incorrect identification bytes. Not in GZIP format");
	public static final ParseException UNSUPPORTED_COMPRESSION_METHOD = new ParseException("Unsupported compression method. Deflate compression required");
	public static final ParseException FEXTRA_TOO_LARGE = new ParseException("FEXTRA part of a header is larger than maximum allowed length");
	public static final ParseException FNAME_FCOMMENT_TOO_LARGE = new ParseException("FNAME or FEXTRA header is larger than maximum allowed length");
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

	private Inflater inflater = new Inflater(true);

	private ByteBufQueue bufs;
	private ByteBufsSupplier input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerGzipInflater() {}

	public static BufsConsumerGzipInflater create() {
		return new BufsConsumerGzipInflater();
	}

	public BufsConsumerGzipInflater withInflater(Inflater inflater) {
		checkArgument(inflater != null, "Cannot use null Inflater");
		this.inflater = inflater;
		return this;
	}

	@Override
	public MaterializedStage<Void> setInput(ByteBufsSupplier input) {
		checkState(this.input == null, "Input already set");
		this.input = sanitize(input);
		this.bufs = input.bufs;
		return getResult();
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
		processHeader();
	}

	private void processHeader() {
		input.parse(ofFixedSize(10))
				.whenResult(buf -> {
							//header validation
							if (buf.get() != GZIP_HEADER[0] || buf.get() != GZIP_HEADER[1]) {
								buf.recycle();
								closeWithError(INCORRECT_ID_HEADER_BYTES);
								return;
							}
							if (buf.get() != GZIP_HEADER[2]) {
								buf.recycle();
								closeWithError(UNSUPPORTED_COMPRESSION_METHOD);
								return;
							}

							byte flag = buf.get();
							if ((flag & 0b11100000) > 0) {
								buf.recycle();
								closeWithError(MALFORMED_FLAG);
								return;
							}
							// unsetting FTEXT bit
							flag &= ~1;
							buf.recycle();
							runNext(flag).run();
						}
				);
	}

	private void processBody() {
		ByteBufQueue queue = new ByteBufQueue();

		while (bufs.hasRemaining()) {
			ByteBuf src = bufs.peekBuf();
			assert src != null;
			inflater.setInput(src.array(), src.readPosition(), src.readRemaining());
			try {
				inflate(queue);
			} catch (DataFormatException e) {
				queue.recycle();
				closeWithError(e);
				return;
			}
			if (inflater.finished()) {
				output.acceptAll(queue.asIterator())
						.whenResult($ -> processFooter());
				return;
			}
		}

		output.acceptAll(queue.asIterator())
				.thenCompose($ -> input.needMoreData())
				.whenResult($1 -> processBody());
	}

	private void processFooter() {
		input.parse(ofFixedSize(GZIP_FOOTER_SIZE))
				.whenResult(buf -> {
					if ((int) crc32.getValue() != reverseBytes(buf.readInt())) {
						closeWithError(CRC32_VALUE_DIFFERS);
						buf.recycle();
						return;
					}

					if (inflater.getTotalOut() != reverseBytes(buf.readInt())) {
						closeWithError(ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED);
						buf.recycle();
						return;
					}
					buf.recycle();
					input.endOfStream()
							.thenCompose($ -> output.accept(null))
							.whenResult($1 -> completeProcess());
				});
	}

	private void inflate(ByteBufQueue queue) throws DataFormatException {
		ByteBuf src = bufs.peekBuf();
		assert src != null;
		while (true) {
			ByteBuf buf = ByteBufPool.allocate(max(src.readRemaining(), DEFAULT_BUF_SIZE));
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
			crc32.update(buf.array(), buf.readPosition(), buf.readRemaining());
			queue.add(buf);
		}
	}

	// region skip header fields
	private void skipHeaders(int flag) {
		// trying to skip optional gzip file members if any is present
		if ((flag & FEXTRA) != 0) {
			skipExtra(flag);
		} else if ((flag & FNAME) != 0) {
			skipTerminatorByte(flag, FNAME);
		} else if ((flag & FCOMMENT) != 0) {
			skipTerminatorByte(flag, FCOMMENT);
		} else if ((flag & FHCRC) != 0) {
			skipCRC16(flag);
		}
	}

	private void skipTerminatorByte(int flag, int part) {
		input.parse(ByteBufsParser.ofNullTerminatedBytes(MAX_HEADER_FIELD_LENGTH))
				.whenException(e -> closeWithError(FNAME_FCOMMENT_TOO_LARGE))
				.whenResult(ByteBuf::recycle)
				.whenResult($ -> runNext(flag - part).run());
	}

	private void skipExtra(int flag) {
		input.parse(ofFixedSize(2))
				.thenApply(shortBuf -> {
					short toSkip = reverseBytes(shortBuf.readShort());
					shortBuf.recycle();
					return toSkip;
				})
				.thenCompose(toSkip -> {
					if (toSkip > MAX_HEADER_FIELD_LENGTH) {
						closeWithError(FEXTRA_TOO_LARGE);
					}
					return input.parse(ofFixedSize(toSkip));
				})
				.whenResult(ByteBuf::recycle)
				.whenResult($ -> runNext(flag - FEXTRA).run());
	}

	private void skipCRC16(int flag) {
		input.parse(ofFixedSize(2))
				.whenResult(ByteBuf::recycle)
				.whenResult($ -> runNext(flag - FHCRC).run());
	}

	private Runnable runNext(int flag) {
		if (flag != 0) {
			return () -> skipHeaders(flag);
		}
		return this::processBody;
	}
	// endregion

	@Override
	protected void doCloseWithError(Throwable e) {
		inflater.end();
		input.closeWithError(e);
		output.closeWithError(e);
	}
}
