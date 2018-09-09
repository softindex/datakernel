package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.stream.BufsConsumerChunkedDecoder.Reading.*;

public final class BufsConsumerChunkedDecoder implements BufsConsumer {
	public static final int DEFAULT_MAX_EXT_TRAILER_LENGTH = 1024; //1 Kb
	public static final int DEFAULT_MAX_CHUNK_SIZE = 1024; //1 Kb
	// region exceptions
	public static final ParseException MALFORMED_CHUNK = new ParseException("Malformed chunk");
	public static final ParseException NOT_ENOUGH_DATA_TO_DECODE = new ParseException("Last chunk has not been received");
	public static final ParseException EXT_TOO_LARGE = new ParseException("Malformed chunk, chunk-ext is larger than maximum allowed length");
	public static final ParseException TRAILER_TOO_LARGE = new ParseException("Malformed chunk, trailer-part is larger than maximum allowed length");
	public static final ParseException CHUNK_SIZE_EXCEEDS_MAXIMUM_SIZE = new ParseException("Chunk size exceeds maximum allowed size");

	// endregion
	enum Reading {
		CHUNK_LENGTH, CHUNK, SKIPPING_EXT, SKIPPING_TRAILER
	}

	private final int maxExtTrailerLength;
	private final int maxChunkSize;
	private final BufsConsumer next;
	private final ByteBufQueue outputBufs = new ByteBufQueue();

	private Reading reading = CHUNK_LENGTH;
	private int currentlySkipped;
	private int chunkSize;

	// region creators
	public BufsConsumerChunkedDecoder(int maxExtTrailerLength, int maxChunkSize, BufsConsumer next) {
		this.next = next;
		this.maxExtTrailerLength = maxExtTrailerLength;
		this.maxChunkSize = maxChunkSize;
	}

	public BufsConsumerChunkedDecoder(BufsConsumer next) {
		this.next = next;
		this.maxExtTrailerLength = DEFAULT_MAX_EXT_TRAILER_LENGTH;
		this.maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
	}
	// endregion

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		try {
			int originalOutputBufSize = outputBufs.remainingBytes();
			while (!inputBufs.isEmpty()) {
				if (reading == CHUNK_LENGTH) {
					if (!readChunkLength(inputBufs)) {
						break;
					}

					currentlySkipped += originalOutputBufSize - outputBufs.remainingBytes();
					if (currentlySkipped > 10) {
						throw CHUNK_SIZE_EXCEEDS_MAXIMUM_SIZE;
					}
				}
				if (reading == SKIPPING_EXT) {
					if (!skip(inputBufs, false)) {
						break;
					}

					currentlySkipped += originalOutputBufSize - outputBufs.remainingBytes();
					if (currentlySkipped > maxExtTrailerLength) {
						throw EXT_TOO_LARGE;
					}

				}
				if (reading == CHUNK) {
					if (!readChunk(inputBufs)) {
						break;
					}
				}
				if (reading == SKIPPING_TRAILER) {
					while (inputBufs.hasRemainingBytes(2)) {
						if (!skip(inputBufs, true)) {
							currentlySkipped += originalOutputBufSize - outputBufs.remainingBytes();

							if (currentlySkipped > maxExtTrailerLength) {
								throw EXT_TOO_LARGE;
							}

							return Stage.of(false);
						}
					}

					return next.push(outputBufs, true)
							.whenException(this::closeWithError);
				}
			}

			// End of stream reached when data is not fully decoded
			if (endOfStream) {
				closeWithError(NOT_ENOUGH_DATA_TO_DECODE);
				return Stage.ofException(NOT_ENOUGH_DATA_TO_DECODE);
			}

			if (outputBufs.remainingBytes() == originalOutputBufSize) {
				return Stage.of(false);
			}

			return next.push(outputBufs, false)
					.whenException(this::closeWithError);

		} catch (ParseException e) {
			closeWithError(e);
			return Stage.ofException(e);
		}
	}

	private boolean readChunk(ByteBufQueue inputBufs) throws ParseException {
		chunkSize -= inputBufs.drainTo(outputBufs, chunkSize);
		if (chunkSize == 0) {
			if (!ensureCRLF(inputBufs)) {
				return false;
			}
			reading = CHUNK_LENGTH;
			return true;
		}
		return false;
	}

	private boolean readChunkLength(ByteBufQueue inputBufs) throws ParseException {
		while (inputBufs.hasRemaining()) {
			byte c = inputBufs.peekByte();
			if (c == SP) {
				inputBufs.getByte();
			} else if (c >= '0' && c <= '9') {
				chunkSize = (chunkSize << 4) + (c - '0');
				inputBufs.getByte();
			} else if (c >= 'a' && c <= 'f') {
				chunkSize = (chunkSize << 4) + (c - 'a' + 10);
				inputBufs.getByte();
			} else if (c >= 'A' && c <= 'F') {
				chunkSize = (chunkSize << 4) + (c - 'A' + 10);
				inputBufs.getByte();
			} else if (c == ';' || c == CR) {
				validateChunkSize();
				reading = SKIPPING_EXT;
				return true;
			} else {
				throw MALFORMED_CHUNK;
			}
			if (chunkSize < 0) {
				throw CHUNK_SIZE_EXCEEDS_MAXIMUM_SIZE;
			}
		}
		return false;
	}

	// Skip including next CRLF
	private boolean skip(ByteBufQueue inputBufs, boolean trailer) throws ParseException {
		int remainingBytes = inputBufs.remainingBytes();
		for (int i = 0; i < remainingBytes - 1; i++) {
			if (inputBufs.peekByte(i) == CR && inputBufs.peekByte(i + 1) == LF) {
				inputBufs.skip(i + 2);
				if (chunkSize != 0) {
					reading = CHUNK;
				} else {
					reading = SKIPPING_TRAILER;
				}
				if (trailer) {
					return i == 0 || !inputBufs.isEmpty();
				}

				return true;
			}
		}
		// skip all bytes or leave 1 byte if it is 'CR'
		int bytesToSkip = remainingBytes - (inputBufs.peekByte(remainingBytes - 1) == CR ? 1 : 0);
		inputBufs.skip(bytesToSkip);

		return false;
	}

	private void validateChunkSize() throws ParseException {
		if (chunkSize > maxChunkSize) {
			throw CHUNK_SIZE_EXCEEDS_MAXIMUM_SIZE;
		}
	}

	private boolean ensureCRLF(ByteBufQueue inputBufs) throws ParseException {
		if (!inputBufs.hasRemainingBytes(2)) {
			return false;
		}
		byte c1 = inputBufs.getByte();
		byte c2 = inputBufs.getByte();
		if (c1 != CR || c2 != LF) {
			throw MALFORMED_CHUNK;
		}
		return true;
	}

	@Override
	public void closeWithError(Throwable e) {
		outputBufs.recycle();
		next.closeWithError(e);
	}

}
