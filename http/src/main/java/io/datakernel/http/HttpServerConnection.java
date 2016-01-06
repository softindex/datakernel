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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.exception.HttpException;
import io.datakernel.http.exception.ServiceIllegalArgumentException;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.util.ByteBufStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import static io.datakernel.http.HttpHeaders.CONNECTION;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.util.ByteBufStrings.SP;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

/**
 * It represents server connection. It can receive requests from clients and respond to them with async servlet.
 */
final class HttpServerConnection extends AbstractHttpConnection {
	private static final Logger logger = LoggerFactory.getLogger(HttpServerConnection.class);
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
	private AsyncHttpServlet servlet;

	/**
	 * Creates a new instance of HttpServerConnection
	 *
	 * @param eventloop     eventloop which will handle its tasks
	 * @param socketChannel channel for this connection
	 * @param servlet       servlet for handling requests
	 * @param pool          pool in which will be stored this connection
	 */
	public HttpServerConnection(NioEventloop eventloop, SocketChannel socketChannel, AsyncHttpServlet servlet, ExposedLinkedList<AbstractHttpConnection> pool, char[] headerChars, int maxHttpMessageSize) {
		super(eventloop, socketChannel, pool, headerChars, maxHttpMessageSize);
		this.servlet = servlet;
		this.remoteAddress = getRemoteSocketAddress().getAddress();
	}

	private static HttpMethod getHttpMethodFromMap(ByteBuf line) {
		int hashCode = 1;
		for (int i = line.position(); i != line.limit(); i++) {
			byte b = line.at(i);
			if (b == SP) {
				for (int p = 0; p < MAX_PROBINGS; p++) {
					int slot = (hashCode + p) & (METHODS.length - 1);
					HttpMethod method = METHODS[slot];
					if (method == null)
						break;
					if (method.compareTo(line.array(), line.position(), i - line.position())) {
						line.advance(method.bytes.length + 1);
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
		if (line.position() == 0) {
			if (line.remaining() >= 4 && line.at(0) == 'G' && line.at(1) == 'E' && line.at(2) == 'T' && line.at(3) == SP) {
				line.advance(4);
				return GET;
			}
			if (line.remaining() >= 5 && line.at(0) == 'P' && line.at(1) == 'O' && line.at(2) == 'S' && line.at(3) == 'T' && line.at(4) == SP) {
				line.advance(5);
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
	protected void onFirstLine(ByteBuf line) {
		assert isRegistered();
		assert eventloop.inEventloopThread();

		HttpMethod method = getHttpMethod(line);
		if (method == null)
			throw new ServiceIllegalArgumentException("Unknown HTTP method");

		request = HttpRequest.create(method);

		int i;
		for (i = 0; i != line.remaining(); i++) {
			byte b = line.peek(i);
			if (b == SP)
				break;
			this.headerChars[i] = (char) b;
		}

		request.url(HttpUri.ofPartialUrl(new String(headerChars, 0, i))); // TODO ?

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
	protected void onHeader(HttpHeader header, final ByteBuf value) {
		super.onHeader(header, value);
		request.addHeader(header, value);
	}

	private void writeHttpResult(HttpResponse httpResponse) {
		if (keepAlive) {
			httpResponse.addHeader(CONNECTION_KEEP_ALIVE);
		}
		ByteBuf buf = httpResponse.write();
		httpResponse.recycleBufs();
		write(buf);
	}

	/**
	 * This method is called after receiving every request. It handles it,
	 * using servlet and sends a response back to the client.
	 * <p>
	 * After sending a response, request and response will be recycled and you can not use it twice.
	 *
	 * @param bodyBuf the received message
	 */
	@Override
	protected void onHttpMessage(ByteBuf bodyBuf) {
		reading = NOTHING;
		request.body(bodyBuf);
		request.remoteAddress(remoteAddress);
		servlet.serveAsync(request, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(final HttpResponse httpResponse) {
				assert eventloop.inEventloopThread();
				if (isRegistered()) {
					writeHttpResult(httpResponse);
				} else {
					// connection is closed, but bufs are not recycled, let's recycle them now
					recycleBufs();
					httpResponse.recycleBufs();
				}
			}

			@Override
			public void onException(final Exception e) {
				assert eventloop.inEventloopThread();
				if (isRegistered()) {
					writeException(e);
				} else {
					// connection is closed, but bufs are not recycled, let's recycle them now
					recycleBufs();
				}
			}
		});
	}

	@Override
	protected void reset() {
		reading = FIRSTLINE;
		readInterest(true);
		keepAlive = false;
		if (request != null) {
			request.recycleBufs();
			request = null;
		}
		super.reset();
	}

	private Runnable readRunnable;

	private void postRead() {
		if (readRunnable == null) {
			readRunnable = new Runnable() {
				@Override
				public void run() {
					onRead();
				}
			};
		}
		eventloop.post(readRunnable);
	}

	@Override
	protected void onWriteFlushed() {
		assert isRegistered();
		if (keepAlive) {
			reset();
			if (readQueue.hasRemaining()) {
				// HTTP Pipelining: since readQueue is already read, onRead() may not be called
				postRead();
			}
		} else {
			close();
			recycleBufs();
		}
	}

	private void writeException(Exception e) {
		writeHttpResult(formatException(e));
	}

	private HttpResponse formatException(Exception e) {
		int status = 500;
		ByteBuf message;
		if (e instanceof HttpException) {
			HttpException httpException = (HttpException) e;
			status = httpException.getStatus();
			message = ByteBufStrings.wrapUTF8(httpException.getMessage());
		} else {
			logger.error("Error processing http request", e);
			message = ByteBuf.wrap(INTERNAL_ERROR_MESSAGE);
		}
		return HttpResponse.create(status).noCache().body(message);
	}

	private void recycleBufs() {
		bodyQueue.clear();
		if (request != null) {
			request.recycleBufs();
			request = null;
		}
	}

	@Override
	public void onClosed() {
		super.onClosed();
		if (reading != NOTHING) {
			// request is not being processed by asynchronous servlet at the moment
			recycleBufs();
		}
		if (connectionsListNode != null) {
			connectionsList.removeNode(connectionsListNode);
			connectionsListNode = null;
		}
	}
}
