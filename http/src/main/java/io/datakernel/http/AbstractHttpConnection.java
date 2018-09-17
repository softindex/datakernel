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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncProcess;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.ParseException;
import io.datakernel.http.stream2.*;
import io.datakernel.serial.*;
import io.datakernel.util.MemSize;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.serial.SerialConsumer.recycle;
import static io.datakernel.util.Preconditions.checkState;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public abstract class AbstractHttpConnection implements AsyncTcpSocket.EventHandler {
	public static final AsyncTimeoutException READ_TIMEOUT_ERROR = new AsyncTimeoutException("HTTP connection read timeout");
	public static final AsyncTimeoutException WRITE_TIMEOUT_ERROR = new AsyncTimeoutException("HTTP connection write timeout");
	public static final ParseException CLOSED_CONNECTION = new ParseException("HTTP connection unexpectedly closed");
	public static final ParseException HEADER_NAME_ABSENT = new ParseException("Header name is absent");
	public static final ParseException TOO_BIG_HTTP_MESSAGE = new ParseException("Too big HttpMessage");
	public static final ParseException TOO_LONG_HEADER = new ParseException("Header line exceeds max header size");
	public static final ParseException TOO_MANY_HEADERS = new ParseException("Too many headers");
	public static final ParseException UNEXPECTED_READ = new ParseException("Unexpected read data");

	public static final MemSize MAX_HEADER_LINE_SIZE = MemSize.kilobytes(8); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = 100; // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields

	protected static final HttpHeaders.Value CONNECTION_KEEP_ALIVE_HEADER = HttpHeaders.asBytes(CONNECTION, "keep-alive");
	protected static final HttpHeaders.Value CONNECTION_CLOSE_HEADER = HttpHeaders.asBytes(CONNECTION, "close");

	private static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	private static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");

	protected final Eventloop eventloop;

	protected final AsyncTcpSocket asyncTcpSocket;
	protected final ByteBufQueue readQueue = new ByteBufQueue();

	protected boolean keepAlive;
	protected int numberOfKeepAliveRequests;

	protected static final byte NOTHING = 0;
	protected static final byte END_OF_STREAM = 1;
	protected static final byte FIRSTLINE = 2;
	protected static final byte HEADERS = 3;
	protected static final byte BODY = 4;
	protected static final byte BODY_SUSPENDED = 5;

	protected byte reading;

	protected static final byte[] CONTENT_ENCODING_GZIP = encodeAscii("gzip");
	private boolean isGzipped = false;

	private boolean isChunked = false;

	protected SettableStage<Void> bodyReader;
	protected SerialSupplier<ByteBuf> bodyWriter;

	protected int contentLength;
	private int maxHeaders;
	protected final char[] headerChars;

	@Nullable
	ConnectionsLinkedList pool;
	@Nullable
	AbstractHttpConnection prev;
	@Nullable
	AbstractHttpConnection next;
	long poolTimestamp;

	/**
	 * Creates a new instance of AbstractHttpConnection
	 *
	 * @param eventloop eventloop which will handle its I/O operations
	 */
	public AbstractHttpConnection(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, char[] headerChars) {
		this.eventloop = eventloop;
		this.headerChars = headerChars;
		assert headerChars.length >= MAX_HEADER_LINE_SIZE.toInt();
		this.asyncTcpSocket = asyncTcpSocket;
		reset();
	}

	protected abstract void onFirstLine(ByteBuf line) throws ParseException;

	protected abstract void onHeadersReceived(SerialSupplier<ByteBuf> bodySupplier);

	protected abstract void onBodyReceived();

	protected abstract void onBodySent();

	protected abstract void onClosed();

	@Override
	public abstract void onClosedWithError(Throwable e);

	protected final boolean isClosed() {
		return pool == null;
	}

	public final void close() {
		if (isClosed()) return;
		asyncTcpSocket.close();
		readQueue.recycle();
		onClosed();
	}

	protected final void closeWithError(Throwable e) {
		if (isClosed()) return;
		onClosedWithError(e);
		asyncTcpSocket.close();
		if (bodyWriter != null) {
			bodyWriter.closeWithError(e);
			bodyWriter = null;
		}
		if (bodyReader != null) {
			bodyReader.setException(e);
			bodyReader = null;
		}
	}

	protected void reset() {
		assert eventloop.inEventloopThread();
		contentLength = 0;
		isChunked = false;
	}

	static SerialSupplier<ByteBuf> createWriter(HttpMessage httpMessage) {
		if (httpMessage.body != null) {
			ByteBuf body = httpMessage.body;
			assert httpMessage.bodySupplier == null;
			if (!httpMessage.useGzip) {
				httpMessage.setHeader(HttpHeaders.ofDecimal(HttpHeaders.CONTENT_LENGTH, body.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + body.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(body);
				body.recycle();
				httpMessage.body = null;
				return SerialSupplier.of(buf);
			} else {
				ByteBuf gzippedBody = GzipProcessorUtils.toGzip(body);
				httpMessage.setHeader(HttpHeaders.asBytes(HttpHeaders.CONTENT_ENCODING, CONTENT_ENCODING_GZIP));
				httpMessage.setHeader(HttpHeaders.ofDecimal(HttpHeaders.CONTENT_LENGTH, gzippedBody.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + gzippedBody.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(gzippedBody);
				gzippedBody.recycle();
				httpMessage.body = null;
				return SerialSupplier.of(buf);
			}
		} else if (httpMessage.bodySupplier != null) {
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			if (!httpMessage.useGzip) {
				BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
				chunker.setInput(httpMessage.bodySupplier);
				SerialSupplier<ByteBuf> result = SerialSuppliers.concat(SerialSupplier.of(buf), chunker.getOutputSupplier());
				chunker.start();
				return result;
			} else {
				BufsConsumerGzipDeflater deflater = BufsConsumerGzipDeflater.create();
				BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
				deflater.setInput(httpMessage.bodySupplier);
				SerialQueue<ByteBuf> queue = new SerialZeroBuffer<>();
				deflater.setOutput(queue.getConsumer());
				chunker.setInput(queue.getSupplier());
				SerialSupplier<ByteBuf> result = SerialSuppliers.concat(SerialSupplier.of(buf), chunker.getOutputSupplier());
				deflater.start();
				chunker.start();
				return result;
			}
		} else {
			httpMessage.setHeader(HttpHeaders.ofDecimal(HttpHeaders.CONTENT_LENGTH, 0));
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			return SerialSupplier.of(buf);
		}
	}

	protected void writeHttpMessage(HttpMessage httpMessage) {
		this.bodyWriter = createWriter(httpMessage);
		writeBody();
		httpMessage.recycle();
	}

	@Override
	public final void onWrite() {
		writeBody();
	}

	private void writeBody() {
		if (bodyWriter != null) {
			bodyWriter.get()
					.whenComplete((buf, e) -> {
						if (e == null) {
							if (buf != null) {
								asyncTcpSocket.write(buf);
							} else {
								bodyWriter = null;
								onBodySent();
							}
						} else {
							closeWithError(e);
						}
					});
		}
	}

	@Nullable
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

	protected void onHeader(HttpHeader header, ByteBuf value) throws ParseException {
		assert !isClosed();
		assert eventloop.inEventloopThread();

		if (header == CONTENT_LENGTH) {
			contentLength = ByteBufStrings.decodeDecimal(value.array(), value.readPosition(), value.readRemaining());
		} else if (header == CONNECTION) {
			keepAlive = equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, value.array(), value.readPosition(), value.readRemaining());
		} else if (header == TRANSFER_ENCODING) {
			isChunked = equalsLowerCaseAscii(TRANSFER_ENCODING_CHUNKED, value.array(), value.readPosition(), value.readRemaining());
		} else if (header == CONTENT_ENCODING) {
			isGzipped = equalsLowerCaseAscii(CONTENT_ENCODING_GZIP, value.array(), value.readPosition(), value.readRemaining());
		}
	}

	private void readBody() throws ParseException {
		assert !isClosed();
		assert eventloop.inEventloopThread();

//		if (readQueue.isEmpty()) return;
		reading = BODY_SUSPENDED;

		SettableStage<Void> bodyReader = this.bodyReader;
		if (bodyReader != null) {
			this.bodyReader = null;
			bodyReader.set(null);
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
		if (isClosed()) return;
		if (readQueue.isEmpty() || !(reading == NOTHING || reading == BODY_SUSPENDED)) {
			asyncTcpSocket.read();
		}
	}

	@Override
	public final void onReadEndOfStream() {
		if (reading == NOTHING) {
			close();
		} else {
			closeWithError(CLOSED_CONNECTION);
		}
	}

	private void doRead() throws ParseException {
		if (reading < BODY) {
			for (; ; ) {
				assert !isClosed();
				assert reading == FIRSTLINE || reading == HEADERS;
				ByteBuf headerBuf = takeHeader();
				if (headerBuf == null) { // states that more bytes are being required
					check(!readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE.toInt()), TOO_LONG_HEADER);
					return;
				}

				if (headerBuf.canRead()) {
					if (reading == FIRSTLINE) {
						onFirstLine(headerBuf);
						reading = HEADERS;
						maxHeaders = MAX_HEADERS;
					} else {
						check(--maxHeaders >= 0, TOO_MANY_HEADERS);
						onHeader(headerBuf);
					}
					continue;
				}

				headerBuf.recycle();
				if (reading == FIRSTLINE)
					throw new ParseException("Empty response from server");
				reading = BODY;
				startReadingBody();
				break;
			}
		}

		if (!isClosed()) {
			readBody();
		}
	}

	private void startReadingBody() {
		ByteBufsInput input;
		AsyncProcess transferDecoder;
		SerialOutput<ByteBuf> transferDecoderOutput;

		if (!isChunked) {
			BufsConsumerDelimiter decoder = BufsConsumerDelimiter.create(contentLength);
			transferDecoder = decoder;
			input = decoder;
			transferDecoderOutput = decoder;
		} else {
			BufsConsumerChunkedDecoder decoder = BufsConsumerChunkedDecoder.create();
			transferDecoder = decoder;
			input = decoder;
			transferDecoderOutput = decoder;
		}

		AsyncProcess contentDecoder;
		SerialOutput<ByteBuf> contentDecoderOutput;

		if (!isGzipped) {
			contentDecoder = null;
			contentDecoderOutput = transferDecoderOutput;
		} else {
			BufsConsumerGzipInflater decoder = BufsConsumerGzipInflater.create();
			contentDecoder = decoder;
			contentDecoderOutput = decoder;
			SerialQueue<ByteBuf> queue = new SerialZeroBuffer<>();
			transferDecoderOutput.setOutput(queue.getConsumer());
			decoder.setInput(queue.getSupplier());
		}

		input.setInput(ByteBufsSupplier.ofProvidedQueue(
				readQueue,
				() -> {
					assert bodyReader == null;
					bodyReader = new SettableStage<>();
					asyncTcpSocket.read();
					return bodyReader;
				},
				Stage::complete,
				this::closeWithError));

		RecyclingSupplier supplier = new RecyclingSupplier(contentDecoderOutput.getOutputSupplier(new SerialZeroBuffer<>()));
		onHeadersReceived(supplier);
		supplier.recycleIfNotUsed();

		transferDecoder.start().both(contentDecoder != null ? contentDecoder.start() : Stage.complete())
				.whenComplete(($, e) -> {
					reading = NOTHING;
					this.bodyReader = null;
					if (e == null) {
						onBodyReceived();
					} else {
						closeWithError(e);
					}
				});
	}

	private static void check(boolean expression, ParseException e) throws ParseException {
		if (!expression) {
			throw e;
		}
	}

	protected void switchPool(ConnectionsLinkedList newPool) {
		if (pool != null) pool.removeNode(this);
		pool = newPool;
		if (pool != null) pool.addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
	}

	@Override
	public String toString() {
		return ", socket=" + asyncTcpSocket +
				", readQueue=" + readQueue +
				", closed=" + isClosed() +
				", keepAlive=" + keepAlive +
				", reading=" + readingToString(reading) +
				", isGzipped=" + isGzipped +
				", isChunked=" + isChunked +
				", contentLengthRemaining=" + contentLength +
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
		}
		return "";
	}

	static final class RecyclingSupplier extends AbstractSerialSupplier<ByteBuf> {
		final SerialSupplier<ByteBuf> supplier;
		Boolean recycling;

		public RecyclingSupplier(SerialSupplier<ByteBuf> supplier) {
			super(supplier);
			this.supplier = supplier;
		}

		@Override
		public Stage<ByteBuf> get() {
			checkState(recycling != Boolean.TRUE);
			recycling = Boolean.FALSE;
			return supplier.get();
		}

		void recycleIfNotUsed() {
			if (recycling == null) {
				recycling = Boolean.TRUE;
				supplier.streamTo(recycle());
			}
		}
	}
}
