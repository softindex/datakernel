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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.TcpSocketConnection;
import io.datakernel.util.ByteBufStrings;
import io.datakernel.util.ExceptionMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static io.datakernel.http.HttpHeader.*;
import static io.datakernel.util.ByteBufStrings.*;

/**
 * Realization of the {@link TcpSocketConnection} which handles the HTTP messages. It is used by server and client.
 */
@SuppressWarnings("ThrowableInstanceNeverThrown")
public abstract class AbstractHttpConnection extends TcpSocketConnection {
	private static final Logger logger = LoggerFactory.getLogger(AbstractHttpConnection.class);

	public static final int DEFAULT_HTTP_BUFFER_SIZE = 16 * 1024;
	public static final int MAX_HEADER_LINE_SIZE = 8 * 1024; // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = 100; // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields

	private static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	private static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");
	protected static final int UNKNOWN_LENGTH = -1;

	public static final RuntimeException HEADER_NAME_ABSENT = new RuntimeException("Header name is absent");
	public static final RuntimeException TOO_BIG_HTTP_MESSAGE = new RuntimeException("Too big HttpMessage");
	public static final RuntimeException MALFORMED_CHUNK = new RuntimeException("Malformed chunk");
	public static final RuntimeException TOO_LONG_HEADER = new RuntimeException("Header line exceeds max header size");
	public static final RuntimeException TOO_MANY_HEADERS = new RuntimeException("Too many headers");

	protected boolean keepAlive = true;

	protected final ByteBufQueue bodyQueue = new ByteBufQueue();

	protected static final byte NOTHING = 0;
	protected static final byte FIRSTLINE = 1;
	protected static final byte HEADERS = 2;
	protected static final byte BODY = 3;
	protected static final byte CHUNK_LENGTH = 4;
	protected static final byte CHUNK = 5;

	protected byte reading;

	private boolean isChunked = false;
	private int chunkSize = 0;

	protected int contentLength;
	private final int maxHttpMessageSize;
	private int maxHeaders;
	private static final int MAX_CHUNK_HEADER_CHARS = 16;
	private int maxChunkHeaderChars;
	protected final char[] headerChars;

	protected final ExposedLinkedList<AbstractHttpConnection> connectionsList;
	protected ExposedLinkedList.Node<AbstractHttpConnection> connectionsListNode;
	private static final ExceptionMarker INTERNAL_MARKER = new ExceptionMarker(AbstractHttpConnection.class, "InternalException");

	/**
	 * Creates a new instance of AbstractHttpConnection
	 *
	 * @param eventloop          eventloop which will handle its I/O operations
	 * @param socketChannel      socket for this connection
	 * @param connectionsList    pool in which will stored this connection
	 * @param maxHttpMessageSize
	 */
	public AbstractHttpConnection(NioEventloop eventloop, SocketChannel socketChannel, ExposedLinkedList<AbstractHttpConnection> connectionsList, char[] headerChars, int maxHttpMessageSize) {
		super(eventloop, socketChannel);
		this.receiveBufferSize = DEFAULT_HTTP_BUFFER_SIZE;
		this.connectionsList = connectionsList;
		this.headerChars = headerChars;
		assert headerChars.length >= MAX_HEADER_LINE_SIZE;
		this.maxHttpMessageSize = maxHttpMessageSize;
		reset();
	}

	/**
	 * After creating this connection adds it to a pool of connections.
	 */
	@Override
	public void onRegistered() {
		assert connectionsListNode == null;
		assert isRegistered();
		assert eventloop.inEventloopThread();

		connectionsListNode = connectionsList.addLastValue(this);
	}

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

	private ByteBuf takeLine() {
		int offset = 0;
		for (int i = 0; i < readQueue.remainingBufs(); i++) {
			ByteBuf buf = readQueue.peekBuf(i);
			for (int p = buf.position(); p < buf.limit(); p++) {
				if (buf.at(p) == LF) {
					ByteBuf line = readQueue.takeExactSize(offset + p - buf.position() + 1);
					if (line.remaining() >= 2 && line.peek(line.remaining() - 2) == CR) {
						line.limit(line.limit() - 2);
					} else {
						line.limit(line.limit() - 1);
					}
					return line;
				}
			}
			offset += buf.remaining();
		}
		return null;
	}

	private void onHeader(ByteBuf line) {
		int pos = line.position();
		int hashCode = 1;
		while (pos < line.limit()) {
			byte b = line.at(pos);
			if (b == ':')
				break;
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			hashCode = 31 * hashCode + b;
			pos++;
		}
		check(pos != line.limit(), HEADER_NAME_ABSENT);
		HttpHeader httpHeader = parseHeader(line.array(), line.position(), pos - line.position(), hashCode);
		pos++;

		// RFC 2616, section 19.3 Tolerant Applications
		while (pos < line.limit() && (line.at(pos) == SP || line.at(pos) == HT)) {
			pos++;
		}
		line.position(pos);
		onHeader(httpHeader, line);
	}

	/**
	 * This method is called after receiving the line of the header.
	 *
	 * @param line received line of header.
	 */
	protected abstract void onFirstLine(ByteBuf line);

	protected void onHeader(HttpHeader header, final ByteBuf value) {
		assert isRegistered();
		assert eventloop.inEventloopThread();

		if (header == CONTENT_LENGTH) {
			contentLength = ByteBufStrings.decodeDecimal(value.array(), value.position(), value.remaining());
			check(contentLength <= maxHttpMessageSize, TOO_BIG_HTTP_MESSAGE);
		} else if (header == CONNECTION) {
			keepAlive = equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, value.array(), value.position(), value.remaining());
		} else if (header == TRANSFER_ENCODING) {
			isChunked = equalsLowerCaseAscii(TRANSFER_ENCODING_CHUNKED, value.array(), value.position(), value.remaining());
		}
	}

	private void readBody() {
		assert isRegistered();
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
				onHttpMessage(bodyQueue.takeRemaining());
			}
		} else {
			assert reading == CHUNK || reading == CHUNK_LENGTH;
			readChunks();
		}
	}

	@SuppressWarnings("ConstantConditions")
	private void readChunks() {
		assert isRegistered();
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
						onHttpMessage(bodyQueue.takeRemaining());
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
	protected void onRead() {
		assert eventloop.inEventloopThread();
		if (reading == NOTHING) {
			readInterest(false);
			return;
		}
		try {
			doRead();
		} catch (Exception e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Could not read HTTP message", e);
			} else {
				if (logger.isWarnEnabled()) {
					logger.warn("Could not read HTTP message: {}", e.toString());
				}
			}
			onInternalException(e);
		}
	}

	private void doRead() {
		if (reading < BODY) {
			while (true) {
				assert isRegistered();
				assert reading == FIRSTLINE || reading == HEADERS;
				ByteBuf line = takeLine();
				if (line == null) {
					check(!readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE), TOO_LONG_HEADER);
					return;
				}

				if (!line.hasRemaining()) {
					if (isChunked) {
						reading = CHUNK_LENGTH;
						maxChunkHeaderChars = MAX_CHUNK_HEADER_CHARS;
					} else
						reading = BODY;
					line.recycle();
					break;
				}

				if (reading == FIRSTLINE) {
					onFirstLine(line);
					line.recycle();
					reading = HEADERS;
					maxHeaders = MAX_HEADERS;
				} else {
					check(--maxHeaders >= 0, TOO_MANY_HEADERS);
					onHeader(line);
				}
			}
		}

		assert isRegistered();
		assert reading >= BODY;
		readBody();
	}

	private static void check(boolean expression, RuntimeException e) {
		if (!expression) {
			throw e;
		}
	}

	@Override
	protected void onInternalException(Exception e) {
		if (e.getClass() == IOException.class) {
			logger.warn("onInternalException in {}: {}", this, e.toString());
			eventloop.updateExceptionCounter(INTERNAL_MARKER, e, this);
			close();
			return;
		}
		super.onInternalException(e);
	}

}
