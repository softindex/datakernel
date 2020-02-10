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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.concurrent.ThreadLocalCharArray;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.parse.UnknownFormatException;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer.Inspector;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.Arrays;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;
import static io.datakernel.http.HttpHeaders.CONNECTION;
import static io.datakernel.http.HttpMessage.MUST_LOAD_BODY;
import static io.datakernel.http.HttpMethod.*;

/**
 * It represents server connection. It can receive {@link HttpRequest requests}
 * from {@link AsyncHttpClient clients} and respond to them with
 * {@link AsyncServlet<HttpRequest> async servlet}.
 */
final class HttpServerConnection extends AbstractHttpConnection {
	private static final int HEADERS_SLOTS = 256;
	private static final int MAX_PROBINGS = 2;
	private static final HttpMethod[] METHODS = new HttpMethod[HEADERS_SLOTS];

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

	private final InetAddress remoteAddress;

	@Nullable
	private HttpRequest request;
	private final AsyncHttpServer server;
	@Nullable
	private final Inspector inspector;
	private final AsyncServlet servlet;
	private final char[] charBuffer;
	private final int maxBodySize;

	private static final byte[] EXPECT_100_CONTINUE = encodeAscii("100-continue");
	private static final byte[] EXPECT_RESPONSE_CONTINUE = encodeAscii("HTTP/1.1 100 Continue\r\n\r\n");

	/**
	 * Creates a new instance of HttpServerConnection
	 *
	 * @param eventloop     eventloop which will handle its tasks
	 * @param remoteAddress an address of remote
	 * @param server        server, which uses this connection
	 * @param servlet       servlet for handling requests
	 */
	HttpServerConnection(Eventloop eventloop, InetAddress remoteAddress, AsyncTcpSocket asyncTcpSocket,
			AsyncHttpServer server, AsyncServlet servlet, char[] charBuffer) {
		super(eventloop, asyncTcpSocket);
		this.server = server;
		this.servlet = servlet;
		this.remoteAddress = remoteAddress;
		this.inspector = server.inspector;
		this.charBuffer = charBuffer;
		this.maxBodySize = server.maxBodySize;
	}

	public void serve() {
		(pool = server.poolNew).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
		socket.read().whenComplete(startLineConsumer);
	}

	@Override
	public void onClosedWithError(@NotNull Throwable e) {
		if (inspector != null) {
			inspector.onHttpError(remoteAddress, e);
		}
	}

	/**
	 * This method is called after received line of header.
	 *
	 * @param line received line of header.
	 */
	@SuppressWarnings("PointlessArithmeticExpression")
	@Override
	protected void onStartLine(byte[] line, int limit) throws ParseException {
		switchPool(server.poolReadWrite);

		HttpMethod method = getHttpMethod(line);
		if (method == null) {
			throw new UnknownFormatException(HttpServerConnection.class,
					"Unknown HTTP method. First Bytes: " + Arrays.toString(line));
		}

		int urlStart = method.size + 1;

		int urlEnd;
		for (urlEnd = urlStart; urlEnd < limit; urlEnd++) {
			if (line[urlEnd] == SP) {
				break;
			}
		}

		int p;
		for (p = urlEnd + 1; p < limit; p++) {
			if (line[p] != SP) {
				break;
			}
		}

		if (p + 7 < limit) {
			boolean http11 = line[p + 0] == 'H' && line[p + 1] == 'T' && line[p + 2] == 'T' && line[p + 3] == 'P'
					&& line[p + 4] == '/' && line[p + 5] == '1' && line[p + 6] == '.' && line[p + 7] == '1';
			if (http11) {
				flags |= KEEP_ALIVE; // keep-alive for HTTP/1.1
			}
		}

		request = new HttpRequest(method,
				UrlParser.parse(decodeAscii(line, urlStart, urlEnd - urlStart, ThreadLocalCharArray.ensure(charBuffer, urlEnd - urlStart))));
		request.maxBodySize = maxBodySize;

		if (method == GET || method == DELETE) {
			contentLength = 0;
		}
	}

	@Override
	protected void onHeaderBuf(ByteBuf buf) {
		//noinspection ConstantConditions
		request.addHeaderBuf(buf);
	}

	private static HttpMethod getHttpMethod(byte[] line) {
		boolean get = line[0] == 'G' && line[1] == 'E' && line[2] == 'T' && line[3] == SP;
		if (get) {
			return GET;
		}
		boolean post = line[0] == 'P' && line[1] == 'O' && line[2] == 'S' && line[3] == 'T' && line[4] == SP;
		if (post) {
			return POST;
		}
		return getHttpMethodFromMap(line);
	}

	private static HttpMethod getHttpMethodFromMap(byte[] line) {
		int hashCode = 1;
		for (int i = 0; i < 10; i++) {
			byte b = line[i];
			if (b == SP) {
				for (int p = 0; p < MAX_PROBINGS; p++) {
					int slot = (hashCode + p) & (METHODS.length - 1);
					HttpMethod method = METHODS[slot];
					if (method == null) {
						break;
					}
					if (method.compareTo(line, 0, i)) {
						return method;
					}
				}
				return null;
			}
			hashCode = 31 * hashCode + b;
		}
		return null;
	}

	/**
	 * This method is called after receiving header. It sets its value to request.
	 *
	 * @param header received header
	 */
	@Override
	protected void onHeader(HttpHeader header, byte[] array, int off, int len) throws ParseException {
		if (header == HttpHeaders.EXPECT) {
			if (equalsLowerCaseAscii(EXPECT_100_CONTINUE, array, off, len)) {
				socket.write(ByteBuf.wrapForReading(EXPECT_RESPONSE_CONTINUE));
			}
		}
		//noinspection ConstantConditions
		if (request.headers.size() >= MAX_HEADERS) {
			throw TOO_MANY_HEADERS;
		}
		request.addHeader(header, array, off, len);
	}

	private void writeHttpResponse(HttpResponse httpResponse) {
		HttpHeaderValue connectionHeader = (flags & KEEP_ALIVE) != 0 ? CONNECTION_KEEP_ALIVE_HEADER : CONNECTION_CLOSE_HEADER;
		if (server.maxKeepAliveRequests != 0) {
			if (++numberOfKeepAliveRequests >= server.maxKeepAliveRequests) {
				connectionHeader = CONNECTION_CLOSE_HEADER;
			}
		}
		httpResponse.addHeader(CONNECTION, connectionHeader);
		ByteBuf buf = renderHttpMessage(httpResponse);
		if (buf != null) {
			if ((flags & KEEP_ALIVE) != 0) {
				eventloop.post(wrapContext(this, () -> writeBuf(buf)));
			} else {
				writeBuf(buf);
			}
		} else {
			writeHttpMessageAsStream(httpResponse);
		}
		httpResponse.recycle();
	}

	@Override
	protected void onHeadersReceived(@Nullable ByteBuf body, @Nullable ChannelSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();

		//noinspection ConstantConditions
		request.flags |= MUST_LOAD_BODY;
		request.body = body;
		request.bodyStream = bodySupplier;
		request.setRemoteAddress(remoteAddress);

		if (inspector != null) {
			inspector.onHttpRequest(request);
		}

		switchPool(server.poolServing);

		HttpRequest request = this.request;
		Promise<HttpResponse> servletResult;
		try {
			servletResult = servlet.serveAsync(request);
		} catch (UncheckedException u) {
			servletResult = Promise.ofException(u.getCause());
		}
		servletResult.whenComplete((response, e) -> {
			assert eventloop.inEventloopThread();
			if (isClosed()) {
				request.recycle();
				if (response != null) {
					response.recycle();
				}
				return;
			}
			if (e == null) {
				if (inspector != null) {
					inspector.onHttpResponse(request, response);
				}
				switchPool(server.poolReadWrite);
				writeHttpResponse(response);
			} else {
				if (inspector != null) {
					inspector.onServletException(request, e);
				}
				switchPool(server.poolReadWrite);
				writeException(e);
			}

			if (request.bodyStream != null) {
				request.bodyStream.streamTo(BUF_RECYCLER);
				request.bodyStream = null;
			}
		});
	}

	@Override
	protected void onBodyReceived() {
		assert !isClosed();
		flags |= BODY_RECEIVED;
		if ((flags & BODY_SENT) != 0 && pool != server.poolServing) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onBodySent() {
		assert !isClosed();
		flags |= BODY_SENT;
		if ((flags & BODY_RECEIVED) != 0 && pool != server.poolServing) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onNoContentLength() {
		throw new AssertionError("This method should not be called on a server");
	}

	private void onHttpMessageComplete() {
		assert !isClosed();

		if (request != null) {
			request.recycle();
			request = null;
		}

		if ((flags & KEEP_ALIVE) != 0 && server.keepAliveTimeoutMillis != 0) {
			switchPool(server.poolKeepAlive);
			flags = 0;
			try {
				/*
					as per RFC 7230, section 3.3.3,
					if no Content-Length header is set, server can assume that a length of a message is 0
				 */
				contentLength = 0;
				readHttpMessage();
			} catch (ParseException e) {
				closeWithError(e);
			}
		} else {
			close();
		}
	}

	private void writeException(Throwable e) {
		writeHttpResponse(server.formatHttpError(e));
	}

	@Override
	protected void onClosed() {
		if (request != null && pool != server.poolServing) {
			request.recycle();
			request = null;
		}
		//noinspection ConstantConditions
		pool.removeNode(this);
		//noinspection AssertWithSideEffects,ConstantConditions
		assert (pool = null) == null;
		server.onConnectionClosed();
	}

	@Override
	public String toString() {
		return "HttpServerConnection{" +
				"remoteAddress=" + remoteAddress +
				',' + super.toString() +
				'}';
	}
}
