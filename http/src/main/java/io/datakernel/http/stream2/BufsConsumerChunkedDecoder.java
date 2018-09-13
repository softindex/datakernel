package io.datakernel.http.stream2;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.processor.AbstractIOAsyncProcess;
import io.datakernel.serial.processor.WithByteBufsInput;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.Math.min;

public final class BufsConsumerChunkedDecoder extends AbstractIOAsyncProcess
		implements WithSerialToSerial<BufsConsumerChunkedDecoder, ByteBuf, ByteBuf>, WithByteBufsInput<BufsConsumerChunkedDecoder> {
	public static final int DEFAULT_MAX_EXT_LENGTH = 1024; //1 Kb
	public static final int DEFAULT_MAX_CHUNK_LENGTH = 1024; //1 Kb
	public static final int MAX_CHUNK_LENGTH_DIGITS = 8;
	// region exceptions
	public static final ParseException MALFORMED_CHUNK = new ParseException("Malformed chunk");
	public static final ParseException MALFORMED_CHUNK_LENGTH = new ParseException("Malformed chunk length");
	public static final ParseException EXT_TOO_LARGE = new ParseException("Malformed chunk, chunk-ext is larger than maximum allowed length");
	public static final ParseException TRAILER_TOO_LARGE = new ParseException("Malformed chunk, trailer-part is larger than maximum allowed length");
	// endregion

	private int maxExtLength = DEFAULT_MAX_EXT_LENGTH;
	private int maxChunkLength = DEFAULT_MAX_CHUNK_LENGTH;

	private ByteBufQueue bufs;
	private ByteBufsSupplier input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerChunkedDecoder() {}

	public static BufsConsumerChunkedDecoder create(){
		return new BufsConsumerChunkedDecoder();
	}

	public BufsConsumerChunkedDecoder withMaxChunkLength(int maxChunkLength){
		this.maxChunkLength = maxChunkLength;
		return this;
	}

	public BufsConsumerChunkedDecoder withMaxExtLength(int maxExtLength){
		this.maxExtLength = maxExtLength;
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
		processLength();
	}

	private void processLength() {
		int remainingBytes = bufs.remainingBytes();
		int chunkLength = 0;
		for (int i = 0; i < min(remainingBytes, MAX_CHUNK_LENGTH_DIGITS); i++) {
			byte c = bufs.peekByte(i);
			if (c >= '0' && c <= '9') {
				chunkLength = (chunkLength << 4) + (c - '0');
			} else if (c >= 'a' && c <= 'f') {
				chunkLength = (chunkLength << 4) + (c - 'a' + 10);
			} else if (c >= 'A' && c <= 'F') {
				chunkLength = (chunkLength << 4) + (c - 'A' + 10);
			} else if (c == ';' || c == CR) {
				// Success
				if (i == 0 || chunkLength > maxChunkLength || chunkLength < 0) {
					closeWithError(MALFORMED_CHUNK_LENGTH);
					return;
				}
				bufs.skip(i);
				if (chunkLength != 0) {
					consumeCRLF(chunkLength);
				} else {
					validateLastChunk();
				}
				return;
			} else {
				closeWithError(MALFORMED_CHUNK_LENGTH);
				return;
			}
		}

		if (remainingBytes > MAX_CHUNK_LENGTH_DIGITS) {
			closeWithError(MALFORMED_CHUNK);
			return;
		}

		input.needMoreData()
				.thenRun(this::processLength);

	}

	private void processData(int chunkLength, ByteBufQueue queue) {
		chunkLength -= bufs.drainTo(queue, chunkLength);
		if (chunkLength != 0) {
			int newChunkLength = chunkLength;
			input.needMoreData()
					.thenRun(() -> processData(newChunkLength, queue));
			return;
		}
		flushQueue(queue);
	}

	private void consumeCRLF(int chunkLength) {
		int remainingBytes = bufs.remainingBytes();
		for (int i = 0; i < min(maxExtLength, remainingBytes - 1); i++) {
			if (bufs.peekByte(i) == CR && bufs.peekByte(i + 1) == LF) {
				bufs.skip(i + 2);
				processData(chunkLength, new ByteBufQueue());
				return;
			}
		}
		if (remainingBytes > maxExtLength) {
			closeWithError(EXT_TOO_LARGE);
			return;
		}

		input.needMoreData()
				.thenRun(() -> consumeCRLF(chunkLength));
	}

	private void flushQueue(ByteBufQueue queue) {
		if (!bufs.hasRemainingBytes(2)) {
			input.needMoreData()
					.thenRun(() -> flushQueue(queue));
			return;
		}

		if (bufs.getByte() != CR || bufs.getByte() != LF) {
			queue.recycle();
			closeWithError(MALFORMED_CHUNK);
			return;
		}

		output.acceptAll(queue.asIterator())
				.thenRun(this::processLength);
	}

	private void validateLastChunk() {
		int remainingBytes = bufs.remainingBytes();
		for (int i = 0; i < min(maxExtLength, remainingBytes - 3); i++) {
			if (bufs.peekByte(i) == CR
					&& bufs.peekByte(i + 1) == LF
					&& bufs.peekByte(i + 2) == CR
					&& bufs.peekByte(i + 3) == LF) {
				bufs.skip(i + 4);

				input.endOfStream()
						.thenCompose($ -> output.accept(null))
						.thenRun(this::completeProcess);
				return;
			}
		}

		if (remainingBytes > maxExtLength) {
			closeWithError(TRAILER_TOO_LARGE);
			return;
		}
		input.needMoreData()
				.thenRun(this::validateLastChunk);
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.closeWithError(e);
		output.closeWithError(e);
	}
}
