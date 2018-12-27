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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.exception.UnknownFormatException;
import io.datakernel.http.AsyncHttpServer.Inspector;

import java.net.InetAddress;
import java.util.Arrays;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpHeaders.CONNECTION;
import static io.datakernel.http.HttpMethod.*;

/**
 * It represents server connection. It can receive {@link HttpRequest requests}
 * from {@link AsyncHttpClient clients} and respond to them with
 * {@link AsyncServlet async servlet}.
 */
final class HttpServerConnection extends AbstractHttpConnection {
	public static final ParseException FIRST_LINE_IS_TOO_BIG = new ParseException(HttpServerConnection.class, "First line is too big");

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

	private HttpRequest request;
	private final AsyncHttpServer server;
	private final Inspector inspector;
	private final AsyncServlet servlet;

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
			AsyncHttpServer server, AsyncServlet servlet) {
		super(eventloop, asyncTcpSocket);
		this.server = server;
		this.servlet = servlet;
		this.remoteAddress = remoteAddress;
		this.inspector = server.inspector;
	}

	public void serve() {
		switchPool(server.poolReadWrite);
		readHttpMessage();
	}

	@Override
	public void onClosedWithError(Throwable e) {
		if (e != null) {
			if (inspector != null) {
				inspector.onHttpError(remoteAddress, e);
			}
			writeException(e);
		}
	}

	private static HttpMethod getHttpMethodFromMap(byte[] line) {
		assert line.length >= 16;
		int hashCode = 1;
		for (int i = 0; i < 10; i++) {
			byte b = line[i];
			if (b == SP) {
				for (int p = 0; p < MAX_PROBINGS; p++) {
					int slot = (hashCode + p) & (METHODS.length - 1);
					HttpMethod method = METHODS[slot];
					if (method == null)
						break;
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

	private static HttpMethod getHttpMethod(byte[] line) {
		if (line[0] == 'G' && line[1] == 'E' && line[2] == 'T' && line[3] == SP) {
			return GET;
		}
		if (line[0] == 'P' && line[1] == 'O' && line[2] == 'S' && line[3] == 'T' && line[4] == SP) {
			return POST;
		}
		return getHttpMethodFromMap(line);
	}

	/**
	 * This method is called after received line of header.
	 *
	 * @param line received line of header.
	 */
	@Override
	protected void onFirstLine(byte[] line, int size) throws ParseException {
		assert line.length >= 16;
		switchPool(server.poolReadWrite);

		HttpMethod method = getHttpMethod(line);
		if (method == null) {
			String firstBytes = Arrays.toString(line);
			throw new UnknownFormatException(HttpServerConnection.class, "Unknown HTTP method. First Bytes: " + firstBytes);
		}

		int readPosition = method.size + 1;

		if (MAX_HEADER_LINE_SIZE_BYTES <= line.length - readPosition) {
			throw FIRST_LINE_IS_TOO_BIG;
		}

		int i;
		for (i = 0; i < line.length - readPosition; i++) {
			byte b = line[readPosition + i];
			if (b == SP)
				break;
		}

		int p;
		for (p = readPosition + i + 1; p < line.length; p++) {
			if (line[p] != SP)
				break;
		}

		if (p + 7 < line.length) {
			if (line[p + 0] == 'H' && line[p + 1] == 'T' && line[p + 2] == 'T' && line[p + 3] == 'P'
					&& line[p + 4] == '/' && line[p + 5] == '1' && line[p + 6] == '.' && line[p + 7] == '1') {
				flags |= KEEP_ALIVE; // keep-alive for HTTP/1.1
			}
		}

		UrlParser url = UrlParser.parse(decodeAscii(line, readPosition, i));
		request = new HttpRequest(method, url);

		if (method == GET || method == DELETE) {
			contentLength = 0;
		}
	}

	/**
	 * This method is called after receiving header. It sets its value to request.
	 *
	 * @param header received header
	 * @param value  value of received header
	 */
	@Override
	protected void onHeader(HttpHeader header, ByteBuf value) throws ParseException {
		super.onHeader(header, value);
		if (header == HttpHeaders.EXPECT) {
			if (equalsLowerCaseAscii(EXPECT_100_CONTINUE, value.array(), value.readPosition(), value.readRemaining())) {
				socket.write(ByteBuf.wrapForReading(EXPECT_RESPONSE_CONTINUE));
			}
		}
		if (request.headers.size() >= MAX_HEADERS) throw TOO_MANY_HEADERS;
		request.addParsedHeader(header, value);
	}

	private void writeHttpResponse(HttpResponse httpResponse) {
		HttpHeaderValue connectionHeader = (flags & KEEP_ALIVE) != 0 ? CONNECTION_KEEP_ALIVE_HEADER : CONNECTION_CLOSE_HEADER;
		if (server.maxKeepAliveRequests != -1) {
			if (++numberOfKeepAliveRequests >= server.maxKeepAliveRequests) {
				connectionHeader = CONNECTION_CLOSE_HEADER;
			}
		}
		httpResponse.addHeader(CONNECTION, connectionHeader);
		writeHttpMessage(httpResponse);
	}

	@Override
	protected void onHeadersReceived(ChannelSupplier<ByteBuf> bodySupplier) {
		request.bodySupplier = bodySupplier;
		request.setRemoteAddress(remoteAddress);

		if (inspector != null) inspector.onHttpRequest(request);

		switchPool(server.poolServing);

		HttpRequest request = this.request;
		Promise<HttpResponse> servletResult;
		try {
			servletResult = servlet.serve(request);
		} catch (UncheckedException u) {
			servletResult = Promise.ofException(u.getCause());
		}
		servletResult.whenComplete((response, e) -> {
			if (isClosed()) {
				request.recycle();
				if (response != null) response.recycle();
				return;
			}
			if (e == null) {
				if (inspector != null) inspector.onHttpResponse(request, response);
				switchPool(server.poolReadWrite);
				writeHttpResponse(response);
			} else {
				if (inspector != null) inspector.onServletException(request, e);
				switchPool(server.poolReadWrite);
				writeException(e);
			}
		});

		if (request.bodySupplier != null && (request.flags & HttpMessage.DETACHED_BODY_STREAM) == 0) {
			request.bodySupplier.streamTo(BUF_RECYCLER);
			request.bodySupplier = null;
		}
	}

	@Override
	protected void onBodyReceived() {
		if ((flags & (BODY_SENT | BODY_RECEIVED)) == (BODY_SENT | BODY_RECEIVED) && pool != server.poolServing) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onBodySent() {
		if ((flags & (BODY_SENT | BODY_RECEIVED)) == (BODY_SENT | BODY_RECEIVED) && pool != server.poolServing) {
			onHttpMessageComplete();
		}
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
			readHttpMessage();
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
		switchPool(null);
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
