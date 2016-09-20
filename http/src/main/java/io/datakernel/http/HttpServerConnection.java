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
import io.datakernel.eventloop.AsyncSslSocket;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Arrays;

import static io.datakernel.bytebuf.ByteBufStrings.SP;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.http.GzipProcessor.toGzip;
import static io.datakernel.http.HttpHeaders.CONNECTION;
import static io.datakernel.http.HttpHeaders.CONTENT_ENCODING;
import static io.datakernel.http.HttpMethod.*;

/**
 * It represents server connection. It can receive requests from clients and respond to them with async servlet.
 */
final class HttpServerConnection extends AbstractHttpConnection {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static final byte[] INTERNAL_ERROR_MESSAGE = encodeAscii("Failed to process request");
	private static final HttpHeaders.Value CONNECTION_KEEP_ALIVE = HttpHeaders.asBytes(CONNECTION, "keep-alive");

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
	private final AsyncHttpServlet servlet;

	/**
	 * Creates a new instance of HttpServerConnection
	 *
	 * @param eventloop eventloop which will handle its tasks
	 * @param server
	 * @param servlet   servlet for handling requests
	 * @param pool      pool in which will be stored this connection
	 */
	private HttpServerConnection(Eventloop eventloop, InetAddress remoteAddress, AsyncTcpSocket asyncTcpSocket,
	                             AsyncHttpServer server, AsyncHttpServlet servlet,
	                             ExposedLinkedList<AbstractHttpConnection> pool, char[] headerChars,
	                             int maxHttpMessageSize) {
		super(eventloop, asyncTcpSocket, headerChars, maxHttpMessageSize);
		this.server = server;
		this.servlet = servlet;
		this.remoteAddress = remoteAddress;
	}

	static HttpServerConnection create(Eventloop eventloop, InetAddress remoteAddress,
	                                   AsyncTcpSocket asyncTcpSocket, AsyncHttpServer server,
	                                   AsyncHttpServlet servlet,
	                                   ExposedLinkedList<AbstractHttpConnection> pool, char[] headerChars,
	                                   int maxHttpMessageSize) {
		return new HttpServerConnection(eventloop, remoteAddress, asyncTcpSocket, server, servlet,
				pool, headerChars, maxHttpMessageSize);
	}

	@Override
	public void onRegistered() {
		super.onRegistered();
		asyncTcpSocket.read();
	}

	@Override
	public void onClosedWithError(Exception e) {
		onClosed();
	}

	private static HttpMethod getHttpMethodFromMap(ByteBuf line) {
		int hashCode = 1;
		for (int i = line.head(); i != line.tail(); i++) {
			byte b = line.at(i);
			if (b == SP) {
				for (int p = 0; p < MAX_PROBINGS; p++) {
					int slot = (hashCode + p) & (METHODS.length - 1);
					HttpMethod method = METHODS[slot];
					if (method == null)
						break;
					if (method.compareTo(line.array(), line.head(), i - line.head())) {
						line.moveHead(method.bytes.length + 1);
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
		if (line.head() == 0) {
			if (line.headRemaining() >= 4 && line.at(0) == 'G' && line.at(1) == 'E' && line.at(2) == 'T' && line.at(3) == SP) {
				line.moveHead(4);
				return GET;
			}
			if (line.headRemaining() >= 5 && line.at(0) == 'P' && line.at(1) == 'O' && line.at(2) == 'S' && line.at(3) == 'T' && line.at(4) == SP) {
				line.moveHead(5);
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
		if (method == null)
			throw new ParseException("Unknown HTTP method");

		request = HttpRequest.ofMethod(method);

		int i;
		for (i = 0; i != line.headRemaining(); i++) {
			byte b = line.peek(i);
			if (b == SP)
				break;
			this.headerChars[i] = (char) b;
		}

		request.setUrl(HttpUri.parseUrl(new String(headerChars, 0, i))); // TODO ?

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
	protected void onHeader(HttpHeader header, final ByteBuf value) throws ParseException {
		super.onHeader(header, value);
		request.addHeader(header, value);
	}

	private void writeHttpResult(HttpResponse httpResponse) {
		if (keepAlive) {
			httpResponse.addHeader(CONNECTION_KEEP_ALIVE);
		}
		ByteBuf buf = httpResponse.write();
		httpResponse.recycleBufs();
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

		// jmx
		server.requestHandlingStarted(this, request);

		try {
			servlet.serveAsync(request, new AsyncHttpServlet.Callback() {
				@Override
				public void onResult(final HttpResponse httpResponse) {
					assert eventloop.inEventloopThread();
					if (!isClosed()) {
						try {
							if (shouldGzip && httpResponse.getBody() != null) {
								httpResponse.setHeader(HttpHeaders.asBytes(CONTENT_ENCODING, CONTENT_ENCODING_GZIP));
								httpResponse.setBody(toGzip(httpResponse.detachBody()));
							}
							writeHttpResult(httpResponse);
						} catch (ParseException e) {
							writeException(new HttpServletError(500));
							server.recordApplicationError();
						}
					} else {
						// connection is closed, but bufs are not recycled, let's recycle them now
						recycleBufs();
						httpResponse.recycleBufs();
					}

					// jmx
					server.requestHandlingFinished(HttpServerConnection.this);
				}

				@Override
				public void onHttpError(HttpServletError httpServletError) {
					assert eventloop.inEventloopThread();
					if (!isClosed()) {
						writeException(httpServletError);
						server.recordApplicationError();
					} else {
						// connection is closed, but bufs are not recycled, let's recycle them now
						recycleBufs();
					}

					// jmx
					server.requestHandlingFinished(HttpServerConnection.this);
				}
			});
		} catch (ParseException e) {
			writeException(new HttpServletError(400, e));
			server.recordApplicationError();
		}
	}

	@Override
	protected void reset() {
		reading = FIRSTLINE;
		keepAlive = false;
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

		// jmx
		boolean isHttpsConnection = asyncTcpSocket.getClass() == AsyncSslSocket.class;

		if (keepAlive) {
			reset();
			returnToPool();
			if (readQueue.hasRemaining()) {
				onRead(null);
			}

			// jmx
			server.recordRequestEvent(isHttpsConnection, true);
		} else {
			close();
			recycleBufs();

			// jmx
			server.recordRequestEvent(isHttpsConnection, false);
		}
	}

	private void writeException(HttpServletError e) {
		writeHttpResult(formatException(e));
	}

	private HttpResponse formatException(HttpServletError e) {
		logger.error("Error processing http request", e);
		ByteBuf message = ByteBuf.wrapForReading(INTERNAL_ERROR_MESSAGE);
		return HttpResponse.ofCode(e.getCode()).withNoCache().withBody(message);
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
		server.addToPool(this);
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
	}

	@Override
	protected void onHttpProtocolError(ParseException e) {
		// jmx
		server.recordHttpProtocolError();
	}
}
