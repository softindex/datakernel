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
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Arrays;

import static io.datakernel.http.GzipProcessor.toGzip;
import static io.datakernel.http.HttpHeaders.CONNECTION;
import static io.datakernel.http.HttpHeaders.CONTENT_ENCODING;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.util.ByteBufStrings.SP;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

@SuppressWarnings("ThrowableInstanceNeverThrown")
final class HttpServerConnection extends AbstractHttpConnection {
	private static final Logger logger = LoggerFactory.getLogger(HttpServerConnection.class);

	public static final ParseException UNKNOWN_HTTP_METHOD_EXCEPTION = new ParseException("Unknown HTTP method");

	private static final byte[] INTERNAL_ERROR_MESSAGE = encodeAscii("Failed to process request");
	private static final HttpHeaders.Value CONNECTION_KEEP_ALIVE = HttpHeaders.asBytes(CONNECTION, "keep-alive");

	private final InetAddress remoteAddress;

	private HttpRequest request;
	private AsyncHttpServlet servlet;

	// http verb methods
	private static final int HEADERS_SLOTS = 256;
	private static final int MAX_PROBINGS = 2;
	private static final HttpMethod[] METHODS = new HttpMethod[HEADERS_SLOTS];

	private static HttpMethod getHttpMethod(ByteBuf line) {
		if (line.getReadPosition() == 0) {
			if (line.remainingToRead() >= 4 && line.at(0) == 'G' && line.at(1) == 'E' && line.at(2) == 'T' && line.at(3) == SP) {
				line.skip(4);
				return GET;
			}
			if (line.remainingToRead() >= 5 && line.at(0) == 'P' && line.at(1) == 'O' && line.at(2) == 'S' && line.at(3) == 'T' && line.at(4) == SP) {
				line.skip(5);
				return POST;
			}
		}
		return getHttpMethodFromMap(line);
	}

	private static HttpMethod getHttpMethodFromMap(ByteBuf line) {
		int hashCode = 1;
		for (int i = line.getReadPosition(); i != line.getWritePosition(); i++) {
			byte b = line.at(i);
			if (b == SP) {
				for (int p = 0; p < MAX_PROBINGS; p++) {
					int slot = (hashCode + p) & (METHODS.length - 1);
					HttpMethod method = METHODS[slot];
					if (method == null)
						break;
					if (method.compareTo(line.array(), line.getReadPosition(), i - line.getReadPosition())) {
						line.skip(method.bytes.length + 1);
						return method;
					}
				}
				return null;
			}
			hashCode = 31 * hashCode + b;
		}
		return null;
	}

	static {
		assert Integer.bitCount(METHODS.length) == 1;
		nxt:
		for (HttpMethod httpMethod : HttpMethod.values()) {
			int hashCode = Arrays.hashCode(httpMethod.bytes);
			for (int p = 0; p < MAX_PROBINGS; p++) {
				int slot = (hashCode + p) & (METHODS.length - 1);
				if (METHODS[slot] == null) {
					METHODS[slot] = httpMethod;
					continue nxt;
				}
			}
			throw new IllegalArgumentException("HTTP METHODS hash collision, try to increase METHODS size");
		}
	}

	// creators
	HttpServerConnection(Eventloop eventloop, InetAddress remoteAddress, AsyncTcpSocket asyncTcpSocket, AsyncHttpServlet servlet, ExposedLinkedList<AbstractHttpConnection> pool, char[] headerChars, int maxHttpMessageSize) {
		super(eventloop, asyncTcpSocket, pool, headerChars, maxHttpMessageSize);
		this.servlet = servlet;
		this.remoteAddress = remoteAddress;
	}

	// miscellaneous
	@Override
	public void onRegistered() {
		super.onRegistered();
		asyncTcpSocket.read();
	}

	@Override
	protected void reset() {
		super.reset();
		reading = FIRSTCHAR;
		keepAlive = false;
		if (request != null) {
			request.recycleBufs();
			request = null;
		}
	}

	// read methods
	@Override
	public void onRead(ByteBuf buf) {
		assert !isClosed();
		if (reading == FIRSTCHAR && connectionNode != null) {
			removeConnectionFromPool();
		}
		super.onRead(buf);
	}

	@Override
	protected void onFirstLine(ByteBuf line) throws ParseException {
		assert eventloop.inEventloopThread();

		HttpMethod method = getHttpMethod(line);
		if (method == null) throw UNKNOWN_HTTP_METHOD_EXCEPTION;

		request = HttpRequest.create(method);

		int i;
		for (i = 0; i != line.remainingToRead(); i++) {
			byte b = line.peek(i);
			if (b == SP)
				break;
			this.headerChars[i] = (char) b;
		}

		request.url(HttpUri.parseUrl(new String(headerChars, 0, i))); // TODO ?

		if (method == GET || method == DELETE) {
			contentLength = 0;
		}
	}

	@Override
	protected void onHeader(HttpHeader header, ByteBuf value) throws ParseException {
		super.onHeader(header, value);
		request.addHeader(header, value);
	}

	@Override
	protected void onHttpMessage(ByteBuf bodyBuf) {
		reading = NOTHING;
		request.body(bodyBuf);
		request.remoteAddress(remoteAddress);
		try {
			servlet.serveAsync(request, new AsyncHttpServlet.Callback() {
				@Override
				public void onResult(HttpResponse httpResponse) {
					assert eventloop.inEventloopThread();
					if (!isClosed()) {
						try {
							if (shouldGzip && httpResponse.getBody() != null) {
								httpResponse.setHeader(CONTENT_ENCODING, CONTENT_ENCODING_GZIP);
								httpResponse.body = toGzip(httpResponse.getBody());
							}
						} catch (ParseException e) {
							writeException(new HttpServletError(500));
						}
						writeHttpResult(httpResponse);
					} else {
						// connection is closed, but bufs are not recycled, let's recycle them now
						recycleBufs();
						httpResponse.recycleBufs();
					}
				}

				@Override
				public void onHttpError(HttpServletError httpServletError) {
					assert eventloop.inEventloopThread();
					if (!isClosed()) {
						writeException(httpServletError);
					} else {
						// connection is closed, but bufs are not recycled, let's recycle them now
						recycleBufs();
					}
				}
			});
		} catch (ParseException e) {
			writeException(new HttpServletError(400, e));
		}
	}

	// write methods
	private void writeHttpResult(HttpResponse httpResponse) {
		if (keepAlive) {
			httpResponse.addHeader(CONNECTION_KEEP_ALIVE);
		}
		ByteBuf buf = httpResponse.write();
		httpResponse.recycleBufs();
		asyncTcpSocket.write(buf);
		if (!keepAlive) {
			asyncTcpSocket.flushAndClose();
			recycleBufs();
		}
	}

	private void writeException(HttpServletError e) {
		writeHttpResult(formatException(e));
	}

	private HttpResponse formatException(HttpServletError e) {
		logger.error("Error processing http request", e);
		ByteBuf message = ByteBuf.wrapForReading(INTERNAL_ERROR_MESSAGE);
		return HttpResponse.create(e.getCode()).noCache().body(message);
	}

	@Override
	public void onWrite() {
		assert !isClosed();
		if (reading != NOTHING) return;
		if (keepAlive) {
			reset();
			asyncTcpSocket.read();
			if (readQueue.hasRemaining()) {
				// HTTP Pipelining: since readQueue is already read, onRead() may not be called
				reading = FIRSTLINE;
				onRead(null);
			} else {
				assert reading == FIRSTCHAR;
				moveConnectionToPool();
			}
		} else {
			close();
			recycleBufs();
		}
	}

	// close methods
	@Override
	public void onClosedWithError(Exception e) {
		onClosed();
	}

	@Override
	protected void onClosed() {
		if (isClosed()) return;
		super.onClosed();
		if (reading != NOTHING) {
			// request is not being processed by asynchronous servlet at the moment
			recycleBufs();
		}
		if (reading == FIRSTCHAR && connectionNode != null) {
			removeConnectionFromPool();
			connectionNode = null;
		}
	}

	private void recycleBufs() {
		bodyQueue.clear();
		if (request != null) {
			request.recycleBufs();
			request = null;
		}
	}
}
