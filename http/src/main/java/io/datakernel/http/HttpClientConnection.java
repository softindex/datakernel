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

import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeoutException;

import static io.datakernel.http.HttpHeader.CONNECTION;
import static io.datakernel.util.ByteBufStrings.SP;
import static io.datakernel.util.ByteBufStrings.decodeDecimal;

/**
 * Realization of {@link AbstractHttpConnection},  which represents a client side and used for sending requests and
 * receiving responses.
 */
@SuppressWarnings("ThrowableInstanceNeverThrown")
final class HttpClientConnection extends AbstractHttpConnection {
	private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();
	private static final Exception CLOSED_CONNECTION = new IOException("Connection is closed");
	private static final HttpHeader.Value CONNECTION_KEEP_ALIVE = HttpHeader.asBytes(CONNECTION, "keep-alive");

	private ResultCallback<HttpResponse> callback;
	private AsyncCancellable cancellable;
	private HttpResponse response;
	private final HttpClientImpl httpClient;
	protected ExposedLinkedList.Node<HttpClientConnection> ipConnectionListNode;

	/**
	 * Creates a new of  HttpClientConnection
	 *
	 * @param eventloop     eventloop which will handle its I/O operations
	 * @param socketChannel channel for this connection
	 * @param httpClient    client which will handle this connection
	 */
	public HttpClientConnection(NioEventloop eventloop, SocketChannel socketChannel, HttpClientImpl httpClient, char[] headerChars, int maxHttpMessageSize) {
		super(eventloop, socketChannel, httpClient.connectionsList, headerChars, maxHttpMessageSize);
		this.httpClient = httpClient;
	}

	@Override
	protected void onReadException(Exception e) {
		onException(e);
		super.onReadException(e);
	}

	@Override
	protected void onWriteException(Exception e) {
		onException(e);
		super.onWriteException(e);
	}

	@Override
	public void onInternalException(Exception e) {
		onException(e);
		super.onInternalException(e);
	}

	private void onException(Exception e) {
		assert eventloop.inEventloopThread();
		if (this.callback != null) {
			ResultCallback<HttpResponse> callback = this.callback;
			this.callback = null;
			callback.onException(e);
		}
	}

	@Override
	protected void onFirstLine(ByteBuf line) {
		if (line.peek(0) != 'H' || line.peek(1) != 'T' || line.peek(2) != 'T' || line.peek(3) != 'P' || line.peek(4) != '/' || line.peek(5) != '1')
			throw new IllegalArgumentException("Invalid status line");

		int sp1;
		if (line.peek(6) == SP) {
			sp1 = line.position() + 7;
		} else if (line.peek(6) == '.' && (line.peek(7) == '1' || line.peek(7) == '0') && line.peek(8) == SP) {
			sp1 = line.position() + 9;
		} else
			throw new IllegalArgumentException("Invalid status line: " + new String(line.array(), line.position(), line.remaining()));

		int sp2;
		for (sp2 = sp1; sp2 < line.limit(); sp2++) {
			if (line.at(sp2) == SP) {
				break;
			}
		}

		response = HttpResponse.create(decodeDecimal(line.array(), sp1, sp2 - sp1));
		if (isNoBodyMessage(response)) {
			// Reset Content-Length for the case keep-alive connection
			contentLength = 0;
		}
	}

	/**
	 * RFC 2616, section 4.4
	 * 1.Any response message which "MUST NOT" include a message-body (such as the 1xx, 204, and 304 responses and any response to a HEAD request) is always
	 * terminated by the first empty line after the header fields, regardless of the entity-header fields present in the message.
	 */
	private static boolean isNoBodyMessage(HttpResponse message) {
		int messageCode = message.getCode();
		return (messageCode >= 100 && messageCode < 200) || messageCode == 204 || messageCode == 304;
	}

	/**
	 * Calls after receiving header, sets this header to httpResponse.
	 *
	 * @param header received header
	 * @param value  value of header
	 */
	@Override
	protected void onHeader(HttpHeader header, ByteBuf value) {
		super.onHeader(header, value);
		response.addHeader(header, value);
	}

	/**
	 * After receiving Http Message it creates {@link HttpResponse}, calls callback with it and recycles
	 * HTTP response.
	 *
	 * @param bodyBuf the received message
	 */
	@Override
	protected void onHttpMessage(ByteBuf bodyBuf) {
		assert isRegistered();
		response.body(bodyBuf);
		ResultCallback<HttpResponse> callback = this.callback;
		this.callback = null;
		callback.onResult(response);
		if (!isRegistered())
			return;
		if (keepAlive) {
			reset();
			httpClient.addToIpPool(this);
		} else {
			close();
		}
	}

	/**
	 * After reading the end of stream it calls method onHttpMessage() for handling result and closes channel.
	 */
	@Override
	public void onReadEndOfStream() {
		assert eventloop.inEventloopThread();
		if (callback != null) {
			if (reading == BODY && contentLength == UNKNOWN_LENGTH) {
				onHttpMessage(bodyQueue.takeRemaining());
			} else {
				onException(CLOSED_CONNECTION);
			}
		}
		close();
	}

	/**
	 * Resets response of this connection for keeping alive it and reusing for other requests.
	 */
	@Override
	protected void reset() {
		reading = NOTHING;
		if (response != null) {
			response.recycleBufs();
			response = null;
		}
		if (cancellable != null) {
			cancellable.cancel();
			cancellable = null;
		}
		super.reset();
	}

	private void writeHttpRequest(HttpRequest httpRequest) {
		if (keepAlive) {
			httpRequest.addHeader(CONNECTION_KEEP_ALIVE);
		}
		ByteBuf buf = httpRequest.write();
		write(buf);
	}

	/**
	 * Sends the request, recycles it and closes connection after timeout
	 *
	 * @param request  request for sending
	 * @param timeout  time after which connection will be closed
	 * @param callback callback for handling result
	 */
	public void request(HttpRequest request, long timeout, ResultCallback<HttpResponse> callback) {
		this.callback = callback;
		writeHttpRequest(request);
		request.recycleBufs();
		scheduleTimeout(timeout);
	}

	private void scheduleTimeout(final long timeoutTime) {
		assert isRegistered();

		cancellable = eventloop.scheduleBackground(timeoutTime, new Runnable() {
			@Override
			public void run() {
				if (isRegistered()) {
					close();
					onException(TIMEOUT_EXCEPTION);
				}
			}
		});
	}

	@Override
	protected void onWriteFlushed() {
		assert isRegistered();
		assert eventloop.inEventloopThread();

		reading = FIRSTLINE;
		readInterest(true);
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
	public void onClosed() {
		super.onClosed();
		bodyQueue.clear();
		if (connectionsListNode != null) {
			connectionsList.removeNode(connectionsListNode);
		}
		if (response != null) {
			response.recycleBufs();
		}
		httpClient.removeFromIpPool(this);
	}

}
