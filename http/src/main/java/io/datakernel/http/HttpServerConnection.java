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
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.SerialSupplier;

import java.net.InetAddress;
import java.util.Arrays;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpMethod.*;

/**
 * It represents server connection. It can receive {@link HttpRequest requests}
 * from {@link AsyncHttpClient clients} and respond to them with
 * {@link AsyncServlet async servlet}.
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

	private HttpRequest request;
	private final AsyncHttpServer server;
	private final AsyncHttpServer.Inspector inspector;
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
			AsyncHttpServer server, AsyncServlet servlet,
			char[] headerChars) {
		super(eventloop, asyncTcpSocket, headerChars);
		this.server = server;
		this.servlet = servlet;
		this.remoteAddress = remoteAddress;
		this.inspector = server.inspector;
	}

	@Override
	public void onRegistered() {
		asyncTcpSocket.read();
		switchPool(server.poolReading);
	}

	@Override
	public void onClosedWithError(Throwable e) {
		if (inspector != null && e != null) inspector.onHttpError(remoteAddress, e);
		readQueue.recycle();
		onClosed();
	}

	private static HttpMethod getHttpMethodFromMap(ByteBuf line) {
		int hashCode = 1;
		for (int i = line.readPosition(); i != line.writePosition(); i++) {
			byte b = line.at(i);
			if (b == SP) {
				for (int p = 0; p < MAX_PROBINGS; p++) {
					int slot = (hashCode + p) & (METHODS.length - 1);
					HttpMethod method = METHODS[slot];
					if (method == null)
						break;
					if (method.compareTo(line.array(), line.readPosition(), i - line.readPosition())) {
						line.moveReadPosition(method.bytes.length + 1);
						return method;
					}
				}
				return null;
			}
			hashCode = 31 * hashCode + b;
		}
		return null;
	}

	private static HttpMethod getHttpMethod(ByteBuf line) {
		if (line.readPosition() == 0) {
			if (line.readRemaining() >= 4 && line.at(0) == 'G' && line.at(1) == 'E' && line.at(2) == 'T' && line.at(3) == SP) {
				line.moveReadPosition(4);
				return GET;
			}
			if (line.readRemaining() >= 5 && line.at(0) == 'P' && line.at(1) == 'O' && line.at(2) == 'S' && line.at(3) == 'T' && line.at(4) == SP) {
				line.moveReadPosition(5);
				return POST;
			}
		}
		return getHttpMethodFromMap(line);
	}

	/**
	 * This method is called after received line of header.
	 *
	 * @param line received line of header.
	 */
	@Override
	protected void onFirstLine(ByteBuf line) throws ParseException {
		switchPool(server.poolReading);

		HttpMethod method = getHttpMethod(line);
		if (method == null) {
			String firstBytes = line.toString();
			line.recycle();
			throw new ParseException("Unknown HTTP method. First Bytes: " + firstBytes);
		}

		if (headerChars.length <= line.readRemaining()) {
			line.recycle();
			throw new ParseException("First line is too big");
		}

		byte[] array = line.array();
		int i;
		for (i = 0; i < line.readRemaining(); i++) {
			byte b = array[line.readPosition() + i];
			if (b == SP)
				break;
			this.headerChars[i] = (char) b;
		}

		int p;
		for (p = line.readPosition() + i + 1; p < line.writePosition(); p++) {
			if (array[p] != SP)
				break;
		}

		keepAlive = false;
		if (p + 7 < line.writePosition()) {
			if (array[p + 0] == 'H' && array[p + 1] == 'T' && array[p + 2] == 'T' && array[p + 3] == 'P'
					&& array[p + 4] == '/' && array[p + 5] == '1' && array[p + 6] == '.' && array[p + 7] == '1') {
				keepAlive = true; // keep-alive for HTTP/1.1
			}
		}

		UrlParser url = UrlParser.parse(new String(headerChars, 0, i));
		request = HttpRequest.of(method, url);

		if (method == GET || method == DELETE) {
			contentLength = 0;
		}

		line.recycle();
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
				asyncTcpSocket.write(ByteBuf.wrapForReading(EXPECT_RESPONSE_CONTINUE));
			}
		}
		request.addHeader(header, value);
	}

	private void writeHttpResult(HttpResponse httpResponse) {
		httpResponse.addHeader(keepAlive ? CONNECTION_KEEP_ALIVE_HEADER : CONNECTION_CLOSE_HEADER);
		writeHttpMessage(httpResponse);
	}

	@Override
	protected void onHeadersReceived(SerialSupplier<ByteBuf> bodySupplier) {
		request.bodySupplier = bodySupplier;
		request.setRemoteAddress(remoteAddress);

		if (inspector != null) inspector.onHttpRequest(request);

		switchPool(server.poolServing);

		servlet.serve(request)
				.whenComplete((response, e) -> {
					if (e == null) {
						if (inspector != null) inspector.onHttpResponse(request, response);
						if (!isClosed()) {
							switchPool(server.poolWriting);
							writeHttpResult(response);
						} else {
							//connection is closed, but bufs are not recycled, let 's recycle them now
							response.recycle();
						}
					} else {
						if (inspector != null) inspector.onServletException(request, e);
						if (!isClosed()) {
							switchPool(server.poolWriting);
							writeException(e);
						}
					}
					recycleBufs();
				});
	}

	@Override
	protected void reset() {
		reading = FIRSTLINE;
		recycleBufs();
		super.reset();
	}

	@Override
	protected void onBodyReceived() {
		if (bodyWriter == null && bodyReader == null && pool != server.poolServing) onHttpMessageComplete();
	}

	@Override
	protected void onBodySent() {
		if (bodyWriter == null && bodyReader == null && pool != server.poolServing) onHttpMessageComplete();
	}

	private void onHttpMessageComplete() {
		assert !isClosed();

		if (keepAlive && server.keepAliveTimeoutMillis != 0) {
			reset();
			switchPool(server.poolKeepAlive);
			reading = FIRSTLINE;
			if (readQueue.hasRemaining()) {
				eventloop.post(() -> onRead(null));
			}
		} else {
			close();
		}
	}

	private void writeException(Throwable e) {
		writeHttpResult(server.formatHttpError(e));
	}

	private void recycleBufs() {
		if (request != null) {
			request.recycle();
			request = null;
		}
	}

	@Override
	protected void onClosed() {
		switchPool(null);
		if (reading != NOTHING) {
			// request is not being processed by asynchronous servlet at the moment
			recycleBufs();
		}
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
