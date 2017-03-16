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
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.ParseException;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.GzipProcessor.fromGzip;
import static io.datakernel.http.HttpHeaders.*;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public abstract class AbstractHttpConnection implements AsyncTcpSocket.EventHandler {
	public static final AsyncTimeoutException READ_TIMEOUT_ERROR = new AsyncTimeoutException("HTTP connection read timeout");
	public static final AsyncTimeoutException WRITE_TIMEOUT_ERROR = new AsyncTimeoutException("HTTP connection write timeout");
	public static final ParseException CLOSED_CONNECTION = new ParseException("HTTP connection unexpectedly closed");
	public static final ParseException HEADER_NAME_ABSENT = new ParseException("Header name is absent");
	public static final ParseException TOO_BIG_HTTP_MESSAGE = new ParseException("Too big HttpMessage");
	public static final ParseException MALFORMED_CHUNK = new ParseException("Malformed chunk");
	public static final ParseException TOO_LONG_HEADER = new ParseException("Header line exceeds max header size");
	public static final ParseException TOO_MANY_HEADERS = new ParseException("Too many headers");
	public static final ParseException UNEXPECTED_READ = new ParseException("Unexpected read data");

	public static final int MAX_HEADER_LINE_SIZE = 8 * 1024; // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = 100; // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields

	protected static final HttpHeaders.Value CONNECTION_KEEP_ALIVE_HEADER = HttpHeaders.asBytes(CONNECTION, "keep-alive");
	protected static final HttpHeaders.Value CONNECTION_CLOSE_HEADER = HttpHeaders.asBytes(CONNECTION, "close");

	private static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	private static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");
	protected static final int UNKNOWN_LENGTH = -1;

	protected final Eventloop eventloop;

	protected final AsyncTcpSocket asyncTcpSocket;
	protected final ByteBufQueue readQueue = ByteBufQueue.create();

	protected boolean keepAlive;

	protected final ByteBufQueue bodyQueue = ByteBufQueue.create();

	protected static final byte NOTHING = 0;
	protected static final byte END_OF_STREAM = 1;
	protected static final byte FIRSTLINE = 2;
	protected static final byte HEADERS = 3;
	protected static final byte BODY = 4;
	protected static final byte CHUNK_LENGTH = 5;
	protected static final byte CHUNK = 6;

	protected byte reading;

	protected static final byte[] CONTENT_ENCODING_GZIP = encodeAscii("gzip");
	private boolean isGzipped = false;
	protected boolean acceptGzip = false;

	private boolean isChunked = false;
	private int chunkSize = 0;

	protected int contentLength;
	private final int maxHttpMessageSize;
	private int maxHeaders;
	private static final int MAX_CHUNK_HEADER_CHARS = 16;
	private int maxChunkHeaderChars;
	protected final char[] headerChars;

	ConnectionsLinkedList pool;
	AbstractHttpConnection prev;
	AbstractHttpConnection next;
	long poolTimestamp;

	/**
	 * Creates a new instance of AbstractHttpConnection
	 *
	 * @param eventloop eventloop which will handle its I/O operations
	 */
	public AbstractHttpConnection(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, char[] headerChars, int maxHttpMessageSize) {
		this.eventloop = eventloop;
		this.headerChars = headerChars;
		assert headerChars.length >= MAX_HEADER_LINE_SIZE;
		this.maxHttpMessageSize = maxHttpMessageSize;
		this.asyncTcpSocket = asyncTcpSocket;
		reset();
	}

	boolean isClosed() {
		return pool == null;
	}

	public final void close() {
		if (isClosed()) return;
		asyncTcpSocket.close();
		readQueue.clear();
		onClosed();
	}

	abstract protected void onClosed();

	protected final void closeWithError(final Exception e) {
		if (isClosed()) return;
		asyncTcpSocket.close();
		onClosedWithError(e);
	}

	@Override
	public abstract void onClosedWithError(Exception e);

	protected void reset() {
		assert eventloop.inEventloopThread();
		contentLength = UNKNOWN_LENGTH;
		isChunked = false;
		bodyQueue.clear();
	}

	/**
	 * This method is called after reading Http message.
	 *
	 * @param bodyBuf the received message
	 */
	protected abstract void onHttpMessage(ByteBuf bodyBuf);

	private ByteBuf takeHeader() {
		int offset = 0;
		for (int i = 0; i < readQueue.remainingBufs(); i++) {
			ByteBuf buf = readQueue.peekBuf(i);
			for (int p = buf.readPosition(); p < buf.writePosition(); p++) {
				if (buf.at(p) == LF) {

					// check if multiline header(CRLF + 1*(SP|HT)) rfc2616#2.2
					if (isMultilineHeader(buf, p)) {
						preprocessMultiLine(buf, p);
						continue;
					}

					ByteBuf line = readQueue.takeExactSize(offset + p - buf.readPosition() + 1);
					if (line.readRemaining() >= 2 && line.peek(line.readRemaining() - 2) == CR) {
						line.moveWritePosition(-2);
					} else {
						line.moveWritePosition(-1);
					}
					return line;
				}
			}
			offset += buf.readRemaining();
		}
		return null;
	}

	private boolean isMultilineHeader(ByteBuf buf, int p) {
		return p + 1 < buf.writePosition() && (buf.at(p + 1) == SP || buf.at(p + 1) == HT) &&
				isDataBetweenStartAndLF(buf, p);
	}

	private boolean isDataBetweenStartAndLF(ByteBuf buf, int p) {
		return !(p == buf.readPosition() || (p - buf.readPosition() == 1 && buf.at(p - 1) == CR));
	}

	private void preprocessMultiLine(ByteBuf buf, int pos) {
		buf.array()[pos] = SP;
		if (buf.at(pos - 1) == CR) {
			buf.array()[pos - 1] = SP;
		}
	}

	private void onHeader(ByteBuf line) throws ParseException {
		int pos = line.readPosition();
		int hashCode = 1;
		while (pos < line.writePosition()) {
			byte b = line.at(pos);
			if (b == ':')
				break;
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			hashCode = 31 * hashCode + b;
			pos++;
		}
		check(pos != line.writePosition(), HEADER_NAME_ABSENT);
		HttpHeader httpHeader = HttpHeaders.of(line.array(), line.readPosition(), pos - line.readPosition(), hashCode);
		pos++;

		// RFC 2616, section 19.3 Tolerant Applications
		while (pos < line.writePosition() && (line.at(pos) == SP || line.at(pos) == HT)) {
			pos++;
		}
		line.readPosition(pos);
		onHeader(httpHeader, line);
	}

	/**
	 * This method is called after receiving the line of the header.
	 *
	 * @param line received line of header.
	 */
	protected abstract void onFirstLine(ByteBuf line) throws ParseException;

	protected void onHeader(HttpHeader header, final ByteBuf value) throws ParseException {
		assert !isClosed();
		assert eventloop.inEventloopThread();

		if (header == CONTENT_LENGTH) {
			contentLength = ByteBufStrings.decodeDecimal(value.array(), value.readPosition(), value.readRemaining());

			if (contentLength > maxHttpMessageSize) {
				value.recycle();
				throw TOO_BIG_HTTP_MESSAGE;
			}
		} else if (header == CONNECTION) {
			keepAlive = equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, value.array(), value.readPosition(), value.readRemaining());
		} else if (header == TRANSFER_ENCODING) {
			isChunked = equalsLowerCaseAscii(TRANSFER_ENCODING_CHUNKED, value.array(), value.readPosition(), value.readRemaining());
		} else if (header == CONTENT_ENCODING) {
			isGzipped = equalsLowerCaseAscii(CONTENT_ENCODING_GZIP, value.array(), value.readPosition(), value.readRemaining());
		} else if (header == ACCEPT_ENCODING) {
			acceptGzip = contains(value, CONTENT_ENCODING_GZIP);
		}
	}

	private boolean contains(ByteBuf value, byte[] bytes) {
		int pos = value.readPosition();
		while (pos < value.writePosition()) {
			if (value.array()[pos] == bytes[0] && value.readRemaining() >= bytes.length) {
				if (equalsLowerCaseAscii(bytes, value.array(), pos, bytes.length)) {
					return true;
				} else {
					pos += bytes.length;
				}
			} else {
				pos++;
			}
		}
		return false;
	}

	private void readBody() throws ParseException {
		assert !isClosed();
		assert eventloop.inEventloopThread();

		if (reading == BODY) {
			if (contentLength == UNKNOWN_LENGTH) {
				check(bodyQueue.remainingBytes() + readQueue.remainingBytes() <= maxHttpMessageSize, TOO_BIG_HTTP_MESSAGE);
				readQueue.drainTo(bodyQueue);
				return;
			}
			int bytesToRead = contentLength - bodyQueue.remainingBytes();
			int actualBytes = readQueue.drainTo(bodyQueue, bytesToRead);
			if (actualBytes == bytesToRead) {
//				if (!readQueue.isEmpty())
//					throw new IllegalStateException("Extra bytes outside of HTTP message");
				onHttpMessage(isGzipped ? fromGzip(bodyQueue.takeRemaining()) : bodyQueue.takeRemaining());
			}
		} else {
			assert reading == CHUNK || reading == CHUNK_LENGTH;
			readChunks();
		}
	}

	@SuppressWarnings("ConstantConditions")
	private void readChunks() throws ParseException {
		assert !isClosed();
		assert eventloop.inEventloopThread();

		while (!readQueue.isEmpty()) {
			if (reading == CHUNK_LENGTH) {
				byte c = readQueue.peekByte();
				if (c == SP) {
					readQueue.getByte();
				} else if (c >= '0' && c <= '9') {
					chunkSize = (chunkSize << 4) + (c - '0');
					readQueue.getByte();
				} else if (c >= 'a' && c <= 'f') {
					chunkSize = (chunkSize << 4) + (c - 'a' + 10);
					readQueue.getByte();
				} else if (c >= 'A' && c <= 'F') {
					chunkSize = (chunkSize << 4) + (c - 'A' + 10);
					readQueue.getByte();
				} else {
					if (chunkSize != 0) {
						if (!readQueue.hasRemainingBytes(2)) {
							break;
						}
						byte c1 = readQueue.getByte();
						byte c2 = readQueue.getByte();
						check(c1 == CR && c2 == LF, MALFORMED_CHUNK);
						check(bodyQueue.remainingBytes() + contentLength <= maxHttpMessageSize, TOO_BIG_HTTP_MESSAGE);
						reading = CHUNK;
					} else {
						if (!readQueue.hasRemainingBytes(4)) {
							break;
						}
						byte c1 = readQueue.getByte();
						byte c2 = readQueue.getByte();
						byte c3 = readQueue.getByte();
						byte c4 = readQueue.getByte();
						check(c1 == CR && c2 == LF && c3 == CR && c4 == LF, MALFORMED_CHUNK);
//						if (!readQueue.isEmpty())
//							throw new IllegalStateException("Extra bytes outside of chunk");
						onHttpMessage(isGzipped ? fromGzip(bodyQueue.takeRemaining()) : bodyQueue.takeRemaining());
						return;
					}
				}
				check(--maxChunkHeaderChars >= 0, MALFORMED_CHUNK);
			}
			if (reading == CHUNK) {
				int read = readQueue.drainTo(bodyQueue, chunkSize);
				chunkSize -= read;
				if (chunkSize == 0) {
					if (!readQueue.hasRemainingBytes(2)) {
						break;
					}
					byte c1 = readQueue.getByte();
					byte c2 = readQueue.getByte();
					check(c1 == CR && c2 == LF, MALFORMED_CHUNK);
					reading = CHUNK_LENGTH;
					maxChunkHeaderChars = MAX_CHUNK_HEADER_CHARS;
				}
			}
		}
	}

	@Override
	public final void onRead(ByteBuf buf) {
		assert eventloop.inEventloopThread();
		assert !isClosed();
		if (buf != null) readQueue.add(buf);

		if (reading == NOTHING) {
			return;
		}
		if (reading == END_OF_STREAM && readQueue.hasRemaining()) {
			closeWithError(UNEXPECTED_READ);
			return;
		}
		if (readQueue.hasRemaining()) {
			try {
				doRead();
			} catch (ParseException e) {
				closeWithError(e);
			}
		}
		if ((reading != NOTHING || readQueue.isEmpty()) && !isClosed()) {
			asyncTcpSocket.read();
		}
	}

	private void doRead() throws ParseException {
		if (reading < BODY) {
			while (true) {
				assert !isClosed();
				assert reading == FIRSTLINE || reading == HEADERS;
				ByteBuf headerBuf = takeHeader();
				if (headerBuf == null) { // states that more bytes are being required
					check(!readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE), TOO_LONG_HEADER);
					return;
				}

				if (!headerBuf.canRead()) {
					headerBuf.recycle();

					if (reading == FIRSTLINE)
						throw new ParseException("Empty response from server");

					if (isChunked) {
						reading = CHUNK_LENGTH;
						maxChunkHeaderChars = MAX_CHUNK_HEADER_CHARS;
					} else
						reading = BODY;

					break;
				}

				if (reading == FIRSTLINE) {
					onFirstLine(headerBuf);
					reading = HEADERS;
					maxHeaders = MAX_HEADERS;
				} else {
					check(--maxHeaders >= 0, TOO_MANY_HEADERS);
					onHeader(headerBuf);
				}
			}
		}

		assert !isClosed();
		assert reading >= BODY;
		readBody();
	}

	private static void check(boolean expression, ParseException e) throws ParseException {
		if (!expression) {
			throw e;
		}
	}

	@Override
	public String toString() {
		return ", socket=" + asyncTcpSocket +
				", readQueue=" + readQueue +
				", closed=" + isClosed() +
				", keepAlive=" + keepAlive +
				", bodyQueue=" + bodyQueue +
				", reading=" + readingToString(reading) +
				", isGzipped=" + isGzipped +
				", acceptGzip=" + acceptGzip +
				", isChunked=" + isChunked +
				", chunkSize=" + chunkSize +
				", contentLength=" + contentLength +
				", poolTimestamp=" + poolTimestamp;
	}

	private String readingToString(byte reading) {
		switch (reading) {
			case NOTHING:
				return "NOTHING";
			case END_OF_STREAM:
				return "END_OF_STREAM";
			case FIRSTLINE:
				return "FIRSTLINE";
			case HEADERS:
				return "HEADERS";
			case BODY:
				return "BODY";
			case CHUNK_LENGTH:
				return "CHUNK_LENGTH";
			case CHUNK:
				return "CHUNK";
		}
		return "";
	}
}
