/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.AsyncProcess;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufConsumer;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.ParseException;
import io.datakernel.http.stream.*;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.MemSize;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;

import static io.datakernel.async.AsyncExecutors.ofMaxRecursiveCalls;
import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpHeaderValue.ofBytes;
import static io.datakernel.http.HttpHeaderValue.ofDecimal;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpUtils.trimAndDecodePositiveInt;
import static java.lang.Math.max;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractHttpConnection {
	public static final AsyncTimeoutException READ_TIMEOUT_ERROR = new AsyncTimeoutException(AbstractHttpConnection.class, "Read timeout");
	public static final AsyncTimeoutException WRITE_TIMEOUT_ERROR = new AsyncTimeoutException(AbstractHttpConnection.class, "Write timeout");
	public static final ParseException HEADER_NAME_ABSENT = new ParseException(AbstractHttpConnection.class, "Header name is absent");
	public static final ParseException TOO_LONG_HEADER = new ParseException(AbstractHttpConnection.class, "Header line exceeds max header size");
	public static final ParseException TOO_MANY_HEADERS = new ParseException(AbstractHttpConnection.class, "Too many headers");
	public static final ParseException INCOMPLETE_MESSAGE = new ParseException(AbstractHttpConnection.class, "Incomplete HTTP message");
	public static final ParseException UNEXPECTED_READ = new ParseException(AbstractHttpConnection.class, "Unexpected read data");

	public static final ChannelConsumer<ByteBuf> BUF_RECYCLER = ChannelConsumer.of(AsyncConsumer.of(ByteBuf::recycle));

	public static final MemSize MAX_HEADER_LINE_SIZE = MemSize.of(ApplicationSettings.getInt(HttpMessage.class, "maxHeaderLineSize", MemSize.kilobytes(8).toInt())); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADER_LINE_SIZE_BYTES = MAX_HEADER_LINE_SIZE.toInt(); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = ApplicationSettings.getInt(HttpMessage.class, "maxHeaders", 100); // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields
	public static final int MAX_RECURSIVE_CALLS = ApplicationSettings.getInt(AbstractHttpConnection.class, "maxRecursiveCalls", 64);

	protected static final HttpHeaderValue CONNECTION_KEEP_ALIVE_HEADER = HttpHeaderValue.of("keep-alive");
	protected static final HttpHeaderValue CONNECTION_CLOSE_HEADER = HttpHeaderValue.of("close");

	private static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	private static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");

	protected final Eventloop eventloop;

	protected final AsyncTcpSocket socket;
	protected final ByteBufQueue readQueue = new ByteBufQueue();

	protected static final byte KEEP_ALIVE = 1 << 0;
	protected static final byte GZIPPED = 1 << 1;
	protected static final byte CHUNKED = 1 << 2;
	protected static final byte BODY_RECEIVED = 1 << 3;
	protected static final byte BODY_SENT = 1 << 4;
	protected static final byte CLOSED = (byte) (1 << 7);
	protected byte flags = 0;

	private final ByteBufConsumer onHeaderBuf = this::onHeaderBuf;

	@Nullable
	protected ConnectionsLinkedList pool;
	@Nullable
	protected AbstractHttpConnection prev;
	@Nullable
	protected AbstractHttpConnection next;
	protected long poolTimestamp;

	protected int numberOfKeepAliveRequests;

	protected static final byte[] CONTENT_ENCODING_GZIP = encodeAscii("gzip");

	protected int contentLength;

	protected final ReadConsumer startLineConsumer = new ReadConsumer() {
		@Override
		public void thenRun() throws ParseException {
			readStartLine();
		}
	};

	protected final ReadConsumer headersConsumer = new ReadConsumer() {
		@Override
		public void thenRun() throws ParseException {
			readHeaders();
		}
	};

	/**
	 * Creates a new instance of AbstractHttpConnection
	 *
	 * @param eventloop eventloop which will handle its I/O operations
	 */
	public AbstractHttpConnection(Eventloop eventloop, AsyncTcpSocket socket) {
		this.eventloop = eventloop;
		this.socket = socket;
	}

	protected abstract void onStartLine(byte[] line, int limit) throws ParseException;

	protected abstract void onHeaderBuf(ByteBuf buf);

	protected abstract void onHeader(HttpHeader header, byte[] array, int off, int len) throws ParseException;

	protected abstract void onHeadersReceived(ChannelSupplier<ByteBuf> bodySupplier);

	protected abstract void onBodyReceived();

	protected abstract void onBodySent();

	protected abstract void onClosed();

	protected abstract void onClosedWithError(Throwable e);

	protected final boolean isClosed() {
		return flags < 0;
	}

	public final void close() {
		if (isClosed()) return;
		flags |= CLOSED;
		onClosed();
		socket.close();
		readQueue.recycle();
	}

	protected final void closeWithError(Throwable e) {
		if (isClosed()) return;
		flags |= CLOSED;
		onClosedWithError(e);
		onClosed();
		socket.close();
		readQueue.recycle();
	}

	protected final void readHttpMessage() throws ParseException {
		contentLength = 0;
		readStartLine();
	}

	private void readStartLine() throws ParseException {
		int size = 1;
		for (int i = 0; i < readQueue.remainingBufs(); i++) {
			ByteBuf buf = readQueue.peekBuf(i);
			for (int p = buf.readPosition(); p < buf.writePosition(); p++) {
				if (buf.at(p) == LF) {
					size += p - buf.readPosition();
					if (i == 0 && buf.readPosition() == 0 && size >= 10) {
						onStartLine(buf.array(), size);
						readQueue.skip(size);
					} else {
						ByteBuf line = ByteBufPool.allocate(max(10, size)); // allocate at least 16 bytes
						readQueue.drainTo(line, size);
						try {
							onStartLine(line.array(), size);
						} finally {
							line.recycle();
						}
					}
					readHeaders();
					return;
				}
			}
			size += buf.readRemaining();
		}
		if (readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE_BYTES)) throw TOO_LONG_HEADER;
		socket.read().whenComplete(startLineConsumer);
	}

	private void readHeaders() throws ParseException {
		assert !isClosed();
		NEXT_HEADER:
		while (true) {
			int size = 1;
			for (int i = 0; i < readQueue.remainingBufs(); i++) {
				ByteBuf buf = readQueue.peekBuf(i);
				byte[] array = buf.array();
				int readPosition = buf.readPosition();
				int writePosition = buf.writePosition();
				for (int p = readPosition; p < writePosition; p++) {
					if (array[p] == LF) {

						// check if multiline header(CRLF + 1*(SP|HT)) rfc2616#2.2
						if (isMultilineHeader(array, readPosition, writePosition, p)) {
							preprocessMultiline(array, p);
							continue;
						}

						if (i == 0) {
							int limit = (p - 1 >= readPosition && array[p - 1] == CR) ? p - 1 : p;
							if (limit != readPosition) {
								processHeaderLine(array, readPosition, limit);
								readQueue.skip(p - readPosition + 1, onHeaderBuf);
								readPosition = buf.readPosition();
							} else {
								onHeaderBuf(buf);
								readQueue.skip(p - readPosition + 1);
								break NEXT_HEADER;
							}
							size = 1;
						} else {
							size += p - readPosition;
							byte[] tmp = new byte[size];
							readQueue.drainTo(tmp, 0, size, onHeaderBuf);
							int limit = (tmp.length - 2 >= 0 && tmp[tmp.length - 2] == CR) ? tmp.length - 2 : tmp.length - 1;
							if (limit != 0) {
								processHeaderLine(tmp, 0, limit);
							} else {
								break NEXT_HEADER;
							}
							continue NEXT_HEADER;
						}
					}
				}
				size += buf.readRemaining();
			}

			if (readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE_BYTES)) throw TOO_LONG_HEADER;
			socket.read().whenComplete(headersConsumer);
			return;
		}

		readBody();
	}

	private static boolean isMultilineHeader(byte[] array, int readPosition, int writePosition, int p) {
		return p + 1 < writePosition && (array[p + 1] == SP || array[p + 1] == HT) &&
				isDataBetweenStartAndLF(array, readPosition, p);
	}

	private static boolean isDataBetweenStartAndLF(byte[] array, int readPosition, int p) {
		return !(p == readPosition || (p - readPosition == 1 && array[p - 1] == CR));
	}

	private static void preprocessMultiline(byte[] array, int p) {
		array[p] = SP;
		if (array[p - 1] == CR) {
			array[p - 1] = SP;
		}
	}

	private void processHeaderLine(byte[] array, int off, int limit) throws ParseException {
		int pos = off;
		int hashCode = 1;
		while (pos < limit) {
			byte b = array[pos];
			if (b == ':')
				break;
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			hashCode = 31 * hashCode + b;
			pos++;
		}
		if (pos == limit) throw HEADER_NAME_ABSENT;
		HttpHeader header = HttpHeaders.of(array, off, pos - off, hashCode);
		pos++;

		// RFC 2616, section 19.3 Tolerant Applications
		while (pos < limit && (array[pos] == SP || array[pos] == HT)) {
			pos++;
		}

		int len = limit - pos;
		if (header == CONTENT_LENGTH) {
			contentLength = trimAndDecodePositiveInt(array, pos, len);
		} else if (header == CONNECTION) {
			flags = (byte) ((flags & ~KEEP_ALIVE) |
					(equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, array, pos, len) ? KEEP_ALIVE : 0));
		} else if (header == TRANSFER_ENCODING) {
			flags |= equalsLowerCaseAscii(TRANSFER_ENCODING_CHUNKED, array, pos, len) ? CHUNKED : 0;
		} else if (header == CONTENT_ENCODING) {
			flags |= equalsLowerCaseAscii(CONTENT_ENCODING_GZIP, array, pos, len) ? GZIPPED : 0;
		}

		onHeader(header, array, pos, len);
	}

	private void readBody() {
		if ((flags & (CHUNKED | GZIPPED)) == 0 && readQueue.hasRemainingBytes(contentLength)) {
			ByteBuf body = readQueue.takeExactSize(contentLength);
			onHeadersReceived(new ChannelSuppliers.ChannelSupplierOfValue<>(body));
			if (isClosed()) return;
			flags |= BODY_RECEIVED;
			onBodyReceived();
			return;
		}

		BinaryChannelSupplier encodedStream = BinaryChannelSupplier.ofProvidedQueue(
				readQueue,
				() -> socket.read()
						.thenComposeEx((buf, e) -> {
							if (e == null) {
								if (buf != null) {
									readQueue.add(buf);
									return Promise.complete();
								} else {
									return Promise.<Void>ofException(INCOMPLETE_MESSAGE);
								}
							} else {
								closeWithError(e);
								return Promise.<Void>ofException(e);
							}
						}),
				Promise::complete,
				this::closeWithError);

		ChannelOutput<ByteBuf> bodyStream;
		AsyncProcess process;

		if ((flags & CHUNKED) == 0) {
			BufsConsumerDelimiter decoder = BufsConsumerDelimiter.create(contentLength);
			process = decoder;
			encodedStream.bindTo(decoder.getInput());
			bodyStream = decoder.getOutput();
		} else {
			BufsConsumerChunkedDecoder decoder = BufsConsumerChunkedDecoder.create();
			process = decoder;
			encodedStream.bindTo(decoder.getInput());
			bodyStream = decoder.getOutput()
					.transformWith(consumer -> consumer.withExecutor(ofMaxRecursiveCalls(MAX_RECURSIVE_CALLS)));
		}

		if ((flags & GZIPPED) != 0) {
			BufsConsumerGzipInflater decoder = BufsConsumerGzipInflater.create();
			process = decoder;
			bodyStream.bindTo(decoder.getInput());
			bodyStream = decoder.getOutput()
					.transformWith(consumer -> consumer.withExecutor(ofMaxRecursiveCalls(MAX_RECURSIVE_CALLS)));
		}

		ChannelSupplier<ByteBuf> supplier = bodyStream.getSupplier(); // process gets started here and can cause connection closing

		if (isClosed()) return;

		onHeadersReceived(supplier);

		process.getProcessResult()
				.whenComplete(($, e) -> {
					if (e == null) {
						flags |= BODY_RECEIVED;
						onBodyReceived();
					} else {
						closeWithError(e);
					}
				});
	}

	@Nullable
	public static ByteBuf renderHttpMessage(HttpMessage httpMessage) {
		if (httpMessage.bodySupplier instanceof ChannelSuppliers.ChannelSupplierOfValue) {
			ByteBuf body = ((ChannelSuppliers.ChannelSupplierOfValue<ByteBuf>) httpMessage.bodySupplier).getValue();
			if ((httpMessage.flags & HttpMessage.USE_GZIP) == 0) {
				httpMessage.addHeader(CONTENT_LENGTH, ofDecimal(body.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + body.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(body);
				body.recycle();
				return buf;
			} else {
				ByteBuf gzippedBody = GzipProcessorUtils.toGzip(body);
				httpMessage.addHeader(CONTENT_ENCODING, ofBytes(CONTENT_ENCODING_GZIP));
				httpMessage.addHeader(CONTENT_LENGTH, ofDecimal(gzippedBody.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + gzippedBody.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(gzippedBody);
				gzippedBody.recycle();
				return buf;
			}
		}

		if (httpMessage.bodySupplier == null) {
			httpMessage.addHeader(CONTENT_LENGTH, ofDecimal(0));
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			return buf;
		}

		return null;
	}

	protected void writeHttpMessageAsChunkedStream(HttpMessage httpMessage) {
		httpMessage.addHeader(TRANSFER_ENCODING, ofBytes(TRANSFER_ENCODING_CHUNKED));
		if ((httpMessage.flags & HttpMessage.USE_GZIP) == 0) {
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
			httpMessage.bodySupplier.bindTo(chunker.getInput());
			writeStream(ChannelSuppliers.concat(ChannelSupplier.of(buf), chunker.getOutput().getSupplier()));
		} else {
			httpMessage.addHeader(CONTENT_ENCODING, ofBytes(CONTENT_ENCODING_GZIP));
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			BufsConsumerGzipDeflater deflater = BufsConsumerGzipDeflater.create();
			BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
			httpMessage.bodySupplier.bindTo(deflater.getInput());
			deflater.getOutput().bindTo(chunker.getInput());
			writeStream(ChannelSuppliers.concat(ChannelSupplier.of(buf), chunker.getOutput().getSupplier()));
		}
	}

	protected void writeBuf(ByteBuf buf) {
		socket.write(buf)
				.whenComplete(($, e2) -> {
					if (isClosed()) return;
					if (e2 == null) {
						flags |= BODY_SENT;
						onBodySent();
					} else {
						closeWithError(e2);
					}
				});
	}

	private void writeStream(ChannelSupplier<ByteBuf> supplier) {
		supplier.get()
				.whenComplete((buf, e) -> {
					if (e == null) {
						if (buf != null) {
							socket.write(buf)
									.whenComplete(($, e2) -> {
										if (isClosed()) return;
										if (e2 == null) {
											writeStream(supplier);
										} else {
											closeWithError(e2);
										}
									});
						} else {
							if (isClosed()) return;
							flags |= BODY_SENT;
							onBodySent();
						}
					} else {
						closeWithError(e);
					}
				});
	}

	protected void switchPool(ConnectionsLinkedList newPool) {
		pool.removeNode(this);
		(pool = newPool).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
	}

	private abstract class ReadConsumer implements BiConsumer<ByteBuf, Throwable> {
		@Override
		public void accept(ByteBuf buf, Throwable e) {
			assert !isClosed() || e != null;
			if (e == null) {
				if (buf != null) {
					readQueue.add(buf);
					try {
						thenRun();
					} catch (ParseException e1) {
						closeWithError(e1);
					}
				} else {
					close();
				}
			} else {
				closeWithError(e);
			}
		}

		public abstract void thenRun() throws ParseException;
	}

	@Override
	public String toString() {
		return ", socket=" + socket +
				", readQueue=" + readQueue +
				", closed=" + isClosed() +
				", keepAlive=" + ((flags & KEEP_ALIVE) != 0) +
				", isGzipped=" + ((flags & GZIPPED) != 0) +
				", isChunked=" + ((flags & CHUNKED) != 0) +
				", contentLengthRemaining=" + contentLength +
				", poolTimestamp=" + poolTimestamp;
	}

}
