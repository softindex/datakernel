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

import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.ByteBufStrings;

import static io.datakernel.http.GzipProcessor.fromGzip;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.util.ByteBufStrings.*;
import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings("ThrowableInstanceNeverThrown")
abstract class AbstractHttpConnection implements AsyncTcpSocket.EventHandler {
	public static final int MAX_HEADER_LINE_SIZE = 8 * 1024; // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = 100; // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields

	private static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	private static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");
	protected static final int UNKNOWN_LENGTH = -1;

	public static final ParseException HEADER_NAME_ABSENT = new ParseException("Header name is absent");
	public static final ParseException TOO_BIG_HTTP_MESSAGE = new ParseException("Too big HttpMessage");
	public static final ParseException MALFORMED_CHUNK = new ParseException("Malformed chunk");
	public static final ParseException TOO_LONG_HEADER = new ParseException("Header line exceeds max header size");
	public static final ParseException TOO_MANY_HEADERS = new ParseException("Too many headers");
	public static final ParseException EMPTY_RESPONSE_FROM_SERVER = new ParseException("Empty response from server");

	protected final Eventloop eventloop;

	protected final AsyncTcpSocket asyncTcpSocket;
	protected final ByteBufQueue readQueue = new ByteBufQueue();

	private boolean closed;
	private long lastUsedTime;

	protected boolean keepAlive = true;

	protected final ByteBufQueue bodyQueue = new ByteBufQueue();

	protected static final byte NOTHING = 0;
	protected static final byte END_OF_STREAM = 1;
	protected static final byte FIRSTCHAR = 2;
	protected static final byte FIRSTLINE = 3;
	protected static final byte HEADERS = 4;
	protected static final byte BODY = 5;
	protected static final byte CHUNK_LENGTH = 6;
	protected static final byte CHUNK = 7;

	protected byte reading;

	protected static final byte[] CONTENT_ENCODING_GZIP = encodeAscii("gzip");
	private boolean isGzipped = false;
	protected boolean shouldGzip = false;

	private boolean isChunked = false;
	private int chunkSize = 0;

	protected int contentLength;
	private final int maxHttpMessageSize;
	private int maxHeaders;
	private static final int MAX_CHUNK_HEADER_CHARS = 16;
	private int maxChunkHeaderChars;
	protected final char[] headerChars;

	protected final ExposedLinkedList<AbstractHttpConnection> connectionPool;
	protected ExposedLinkedList.Node<AbstractHttpConnection> connectionNode;

	// creators
	AbstractHttpConnection(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                       ExposedLinkedList<AbstractHttpConnection> connectionPool,
	                       char[] headerChars, int maxHttpMessageSize) {
		this.eventloop = checkNotNull(eventloop);
		this.asyncTcpSocket = checkNotNull(asyncTcpSocket);
		this.connectionPool = checkNotNull(connectionPool);
		this.headerChars = checkNotNull(headerChars);
		assert headerChars.length >= MAX_HEADER_LINE_SIZE;
		this.maxHttpMessageSize = maxHttpMessageSize;
		reset();
	}

	// miscellaneous
	@Override
	public void onRegistered() {
		assert !isClosed();
		assert eventloop.inEventloopThread();
	}

	protected void reset() {
		assert eventloop.inEventloopThread();
		this.lastUsedTime = eventloop.currentTimeMillis();
		contentLength = UNKNOWN_LENGTH;
		isChunked = false;
		bodyQueue.clear();
	}

	// close methods
	protected boolean isClosed() {
		return closed;
	}

	public final void close() {
		asyncTcpSocket.close();
		readQueue.clear();
		onClosed();
		cleanUpPool();
	}

	protected void onClosed() {
		closed = true;
	}

	protected final void closeWithError(final Exception e) {
		if (isClosed()) return;
		eventloop.recordIoError(e, this);
		asyncTcpSocket.close();
		readQueue.clear();
		onClosedWithError(e);
	}

	protected abstract void cleanUpPool();

	// read methods
	@Override
	public void onRead(ByteBuf buf) {
		assert eventloop.inEventloopThread();
		assert !isClosed();

		if (buf != null)
			readQueue.add(buf);

		if (reading == NOTHING)
			return;

		if (reading == FIRSTCHAR) reading = FIRSTLINE;

		try {
			if (readQueue.hasRemaining()) {
				doRead();
			}

			if ((reading != NOTHING || readQueue.isEmpty()) && !isClosed())
				asyncTcpSocket.read();

		} catch (ParseException e) {
			closeWithError(e);
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
						throw EMPTY_RESPONSE_FROM_SERVER;

					if (isChunked) {
						reading = CHUNK_LENGTH;
						maxChunkHeaderChars = MAX_CHUNK_HEADER_CHARS;
					} else
						reading = BODY;

					break;
				}

				if (reading == FIRSTLINE) {
					onFirstLine(headerBuf);
					headerBuf.recycle();
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

	private ByteBuf takeHeader() {
		int offset = 0;
		for (int i = 0; i < readQueue.remainingBufs(); i++) {
			ByteBuf buf = readQueue.peekBuf(i);
			for (int p = buf.getReadPosition(); p < buf.getWritePosition(); p++) {
				if (buf.at(p) == LF) {

					// check if multiline header(CRLF + 1*(SP|HT)) rfc2616#2.2
					if (isMultilineHeader(buf, p)) {
						preprocessMultiLine(buf, p);
						continue;
					}

					ByteBuf line = readQueue.takeExactSize(offset + p - buf.getReadPosition() + 1);
					if (line.remainingToRead() >= 2 && line.peek(line.remainingToRead() - 2) == CR) {
						line.setWritePosition(line.getWritePosition() - 2);
					} else {
						line.setWritePosition(line.getWritePosition() - 1);
					}
					return line;
				}
			}
			offset += buf.remainingToRead();
		}
		return null;
	}

	private boolean isMultilineHeader(ByteBuf buf, int p) {
		return p + 1 < buf.getWritePosition() && (buf.at(p + 1) == SP || buf.at(p + 1) == HT) &&
				isDataBetweenStartAndLF(buf, p);
	}

	private boolean isDataBetweenStartAndLF(ByteBuf buf, int p) {
		return !(p == buf.getReadPosition() || (p - buf.getReadPosition() == 1 && buf.at(p - 1) == CR));
	}

	private void preprocessMultiLine(ByteBuf buf, int pos) {
		buf.set(pos, SP);
		if (buf.at(pos - 1) == CR) {
			buf.set(pos - 1, SP);
		}
	}

	protected abstract void onFirstLine(ByteBuf line) throws ParseException;

	private void onHeader(ByteBuf line) throws ParseException {
		int pos = line.getReadPosition();
		int hashCode = 1;
		while (pos < line.getWritePosition()) {
			byte b = line.at(pos);
			if (b == ':')
				break;
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			hashCode = 31 * hashCode + b;
			pos++;
		}
		check(pos != line.getWritePosition(), HEADER_NAME_ABSENT);
		HttpHeader httpHeader = HttpHeaders.of(line.array(), line.getReadPosition(), pos - line.getReadPosition(), hashCode);
		pos++;

		// RFC 2616, section 19.3 Tolerant Applications
		while (pos < line.getWritePosition() && (line.at(pos) == SP || line.at(pos) == HT)) {
			pos++;
		}
		line.setReadPosition(pos);
		onHeader(httpHeader, line);
	}

	protected void onHeader(HttpHeader header, final ByteBuf value) throws ParseException {
		assert !isClosed();
		assert eventloop.inEventloopThread();

		if (header == CONTENT_LENGTH) {
			contentLength = ByteBufStrings.decodeDecimal(value.array(), value.getReadPosition(), value.remainingToRead());

			if (contentLength > maxHttpMessageSize) {
				value.recycle();
				throw TOO_BIG_HTTP_MESSAGE;
			}
		} else if (header == CONNECTION) {
			keepAlive = equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, value.array(), value.getReadPosition(), value.remainingToRead());
		} else if (header == TRANSFER_ENCODING) {
			isChunked = equalsLowerCaseAscii(TRANSFER_ENCODING_CHUNKED, value.array(), value.getReadPosition(), value.remainingToRead());
		} else if (header == CONTENT_ENCODING) {
			isGzipped = equalsLowerCaseAscii(CONTENT_ENCODING_GZIP, value.array(), value.getReadPosition(), value.remainingToRead());
		} else if (header == ACCEPT_ENCODING) {
			shouldGzip = contains(value, CONTENT_ENCODING_GZIP);
		}
	}

	private boolean contains(ByteBuf value, byte[] bytes) {
		int pos = value.getReadPosition();
		while (pos < value.getWritePosition()) {
			if (value.array()[pos] == bytes[0] && value.remainingToRead() >= bytes.length) {
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
				onHttpMessage(isGzipped ? fromGzip(bodyQueue.takeRemaining()) : bodyQueue.takeRemaining());
			}
		} else {
			assert reading == CHUNK || reading == CHUNK_LENGTH;
			readChunks();
		}
	}

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

	public void onShutdownInput() {
		close();
	}

	protected abstract void onHttpMessage(ByteBuf bodyBuf);

	// connections pool management
	protected void moveConnectionToPool() {
		if (connectionNode == null) {
			connectionNode = new ExposedLinkedList.Node<>(this);
		}
		connectionPool.addLastNode(connectionNode);
	}

	protected void removeConnectionFromPool() {
		connectionPool.removeNode(connectionNode);
	}

	final long getLastUsedTime() {
		return lastUsedTime;
	}

	private static void check(boolean expression, ParseException e) throws ParseException {
		if (!expression) {
			throw e;
		}
	}
}
