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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;

import java.net.InetAddress;
import java.util.Arrays;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.GzipProcessor.toGzip;
import static io.datakernel.http.HttpHeaders.CONTENT_ENCODING;
import static io.datakernel.http.HttpMethod.*;

/**
 * It represents server connection. It can receive requests from clients and respond to them with async servlet.
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
	private final boolean gzipResponses;

	private static final byte[] EXPECT_100_CONTINUE = encodeAscii("100-continue");
	private static final byte[] EXPECT_RESPONSE_CONTINUE = encodeAscii("HTTP/1.1 100 Continue\r\n\r\n");
	private boolean statusExpectContinue;

	/**
	 * Creates a new instance of HttpServerConnection
	 * @param eventloop eventloop which will handle its tasks
	 * @param server
	 * @param servlet   servlet for handling requests
	 * @param gzipResponses
	 */
	HttpServerConnection(Eventloop eventloop, InetAddress remoteAddress, AsyncTcpSocket asyncTcpSocket,
	                     AsyncHttpServer server, AsyncServlet servlet,
	                     char[] headerChars, int maxHttpMessageSize, boolean gzipResponses) {
		super(eventloop, asyncTcpSocket, headerChars, maxHttpMessageSize);
		this.server = server;
		this.servlet = servlet;
		this.remoteAddress = remoteAddress;
		this.inspector = server.inspector;
		this.gzipResponses = gzipResponses;
	}

	@Override
	public void onRegistered() {
		super.onRegistered();
		asyncTcpSocket.read();
	}

	@Override
	public void onClosedWithError(Exception e) {
		super.onClosedWithError(e);
		if (inspector != null) inspector.onConnectionError(remoteAddress, e);
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
		assert eventloop.inEventloopThread();

		if (isInPool()) {
			removeFromPool();
		}

		HttpMethod method = getHttpMethod(line);
		if (method == null) {
			line.recycle();
			throw new ParseException("Unknown HTTP method. First Bytes: " + line.toString(20));
		}

		if (headerChars.length <= line.readRemaining()) {
			line.recycle();
			throw new ParseException("First line is too big");
		}

		int i;
		for (i = 0; i != line.readRemaining(); i++) {
			byte b = line.peek(i);
			if (b == SP) {
				line.moveReadPosition(i);
				break;
			}
			this.headerChars[i] = (char) b;
		}

		HttpUri url = HttpUri.parseUrl(new String(headerChars, 0, i)); // TODO ?

		keepAlive = false;
		if (line.readRemaining() > 8) {
			if (line.peek(1) == 'H' && line.peek(2) == 'T' && line.peek(3) == 'T' && line.peek(4) == 'P'
					&& line.peek(5) == '/' && line.peek(6) == '1' && line.peek(7) == '.') {
				keepAlive = (line.peek(8) == '1'); // keep-alive for HTTP/1.1
			}
		}

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
	protected void onHeader(HttpHeader header, final ByteBuf value) throws ParseException {
		super.onHeader(header, value);
		if (header == HttpHeaders.EXPECT) {
			if (equalsLowerCaseAscii(EXPECT_100_CONTINUE, value.array(), value.readPosition(), value.readRemaining())) {
				statusExpectContinue = true;
				asyncTcpSocket.write(ByteBuf.wrapForReading(EXPECT_RESPONSE_CONTINUE));
			}
		}
		request.addHeader(header, value);
	}

	private void writeHttpResult(HttpResponse httpResponse) {
		httpResponse.addHeader(keepAlive ? CONNECTION_KEEP_ALIVE_HEADER : CONNECTION_CLOSE_HEADER);
		ByteBuf buf = httpResponse.toByteBuf();
		httpResponse.recycleBufs();
		statusExpectContinue = false;
		asyncTcpSocket.write(buf);
	}

	/**
	 * This method is called after receiving every request. It handles it,
	 * using servlet and sends a response back to the client.
	 * <p/>
	 * After sending a response, request and response will be recycled and you can not use it twice.
	 *
	 * @param bodyBuf the received message
	 */
	@Override
	protected void onHttpMessage(ByteBuf bodyBuf) {
		reading = NOTHING;
		request.setBody(bodyBuf);
		request.setRemoteAddress(remoteAddress);

		if (inspector != null) inspector.onHttpRequest(request);

		servlet.serve(request, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(final HttpResponse httpResponse) {
				assert eventloop.inEventloopThread();
				if (inspector != null) inspector.onHttpResponse(request, httpResponse);

				if (!isClosed()) {
					if (acceptGzip && httpResponse.getBody() != null && gzipResponses) {
						httpResponse.setHeader(HttpHeaders.asBytes(CONTENT_ENCODING, CONTENT_ENCODING_GZIP));
						httpResponse.setBody(toGzip(httpResponse.detachBody()));
					}
					writeHttpResult(httpResponse);
				} else {
					// connection is closed, but bufs are not recycled, let's recycle them now
					httpResponse.recycleBufs();
				}
				recycleBufs();
			}

			@Override
			protected void onException(Exception e) {
				assert eventloop.inEventloopThread();
				if (inspector != null) inspector.onServletError(request, e);
				if (!isClosed()) {
					writeException(e);
				}
				recycleBufs();
			}
		});
	}

	@Override
	protected void reset() {
		reading = FIRSTLINE;
		if (request != null) {
			request.recycleBufs();
			request = null;
		}
		super.reset();
	}

	@Override
	public void onWrite() {
		assert !isClosed();
		if (reading != NOTHING) return;

		if (statusExpectContinue) {
			return;
		}

		if (keepAlive) {
			reset();
			returnToPool();
			if (readQueue.hasRemaining()) {
				onRead(null);
			}
		} else {
			close();
		}
	}

	private void writeException(Exception e) {
		writeHttpResult(server.formatHttpError(e));
	}

	private void recycleBufs() {
		bodyQueue.clear();
		if (request != null) {
			request.recycleBufs();
			request = null;
		}
	}

	@Override
	protected void returnToPool() {
		super.returnToPool();
		server.returnToPool(this);
	}

	@Override
	protected void removeFromPool() {
		super.removeFromPool();
		server.removeFromPool(this);
	}

	@Override
	protected void onClosed() {
		super.onClosed();
		if (reading != NOTHING) {
			// request is not being processed by asynchronous servlet at the moment
			recycleBufs();
		}

		if (inspector != null) inspector.onConnectionClosed(remoteAddress);
	}

	@Override
	public String toString() {
		return "HttpServerConnection{" +
				"remoteAddress=" + remoteAddress +
				',' + super.toString() +
				'}';
	}
}
