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
import io.datakernel.net.CloseWithoutNotifyException;

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.SP;
import static io.datakernel.bytebuf.ByteBufStrings.decodeDecimal;
import static io.datakernel.http.HttpHeaders.CONNECTION;

@SuppressWarnings("ThrowableInstanceNeverThrown")
final class HttpClientConnection extends AbstractHttpConnection {
	private static final HttpHeaders.Value CONNECTION_KEEP_ALIVE = HttpHeaders.asBytes(CONNECTION, "keep-alive");

	private ResultCallback<HttpResponse> callback;
	private HttpResponse response;
	private final AsyncHttpClient client;
	private final AsyncHttpClient.Inspector inspector;

	final InetSocketAddress remoteAddress;
	HttpClientConnection addressPrev;
	HttpClientConnection addressNext;

	private Exception closeError;

	HttpClientConnection(Eventloop eventloop, InetSocketAddress remoteAddress,
	                     AsyncTcpSocket asyncTcpSocket, AsyncHttpClient client, char[] headerChars,
	                     int maxHttpMessageSize) {
		super(eventloop, asyncTcpSocket, headerChars, maxHttpMessageSize);
		this.remoteAddress = remoteAddress;
		this.client = client;
		this.inspector = client.inspector;
	}

	@Override
	public void onRegistered() {
	}

	@Override
	public void onClosedWithError(Exception e) {
		if (reading == BODY
				&& contentLength == UNKNOWN_LENGTH
				&& e instanceof CloseWithoutNotifyException) {
			onReadEndOfStream();
			return;
		}
		if (inspector != null && e != null) inspector.onHttpError(this, callback == null, e);
		readQueue.clear();
		if (callback != null) {
			callback.postException(eventloop, e);
			callback = null;
		} else {
			closeError = e;
		}
		onClosed();
	}

	@Override
	protected void onFirstLine(ByteBuf line) throws ParseException {
		if (line.peek(0) != 'H' || line.peek(1) != 'T' || line.peek(2) != 'T' || line.peek(3) != 'P' || line.peek(4) != '/' || line.peek(5) != '1') {
			line.recycle();
			throw new ParseException("Invalid response");
		}

		keepAlive = false;
		int sp1;
		if (line.peek(6) == SP) {
			sp1 = line.readPosition() + 7;
		} else if (line.peek(6) == '.' && (line.peek(7) == '1' || line.peek(7) == '0') && line.peek(8) == SP) {
			if (line.peek(7) == '1')
				keepAlive = true;
			sp1 = line.readPosition() + 9;
		} else {
			line.recycle();
			throw new ParseException("Invalid response: " + new String(line.array(), line.readPosition(), line.readRemaining()));
		}

		int sp2;
		for (sp2 = sp1; sp2 < line.writePosition(); sp2++) {
			if (line.at(sp2) == SP) {
				break;
			}
		}

		int statusCode = decodeDecimal(line.array(), sp1, sp2 - sp1);
		if (!(statusCode >= 100 && statusCode < 600)) {
			line.recycle();
			throw new ParseException("Invalid HTTP Status Code " + statusCode);
		}
		response = HttpResponse.ofCode(statusCode);
		if (isNoBodyMessage(response)) {
			// Reset Content-Length for the case keep-alive connection
			contentLength = 0;
		}

		line.recycle();
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

	@Override
	protected void onHeader(HttpHeader header, ByteBuf value) throws ParseException {
		super.onHeader(header, value);
		response.addHeader(header, value);
	}

	@Override
	protected void onHttpMessage(ByteBuf bodyBuf) {
		assert !isClosed();
		final ResultCallback<HttpResponse> callback = this.callback;
		final HttpResponse response = this.response;
		this.response = null;
		this.callback = null;
		response.setBody(bodyBuf);
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.setResult(response);
				response.recycleBufs();
			}
		});

		if (inspector != null) inspector.onHttpResponse(this, response);
		if (keepAlive && client.keepAliveTimeoutMillis != 0) {
			reset();
			client.returnToKeepAlivePool(this);
		} else {
			close();
		}
	}

	@Override
	public void onReadEndOfStream() {
		if (callback != null) {
			if ((reading == BODY || reading == HEADERS) && contentLength == UNKNOWN_LENGTH) {
				onHttpMessage(bodyQueue.takeRemaining());
				assert callback == null;
			} else {
				closeWithError(CLOSED_CONNECTION);
				assert callback == null;
			}
		}
		close();
	}

	@Override
	protected void reset() {
		reading = END_OF_STREAM;
		if (response != null) {
			response.recycleBufs();
			response = null;
		}
		super.reset();
	}

	/**
	 * Sends the request, recycles it and closes connection in case of timeout
	 *
	 * @param request  request for sending
	 * @param callback callback for handling result
	 */
	public void send(HttpRequest request, ResultCallback<HttpResponse> callback) {
		this.callback = callback;
		assert pool == null;
		(pool = client.poolWriting).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
		request.addHeader(CONNECTION_KEEP_ALIVE);
		asyncTcpSocket.write(request.toByteBuf());
		request.recycleBufs();
	}

	@Override
	public void onWrite() {
		assert !isClosed();
		reading = FIRSTLINE;
		assert pool == client.poolWriting;
		pool.removeNode(this);
		(pool = client.poolReading).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
		asyncTcpSocket.read();
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	protected void onClosed() {
		assert callback == null;
		if (pool == client.poolKeepAlive) {
			AddressLinkedList addresses = client.addresses.get(remoteAddress);
			addresses.removeNode(this);
			if (addresses.isEmpty()) {
				client.addresses.remove(remoteAddress);
			}
		}

		// pool will be null if socket was closed by the peer just before connection.send() invocation
		if (pool != null) {
			pool.removeNode(this);
			pool = null;
		}

		client.onConnectionClosed();
		bodyQueue.clear();
		if (response != null) {
			response.recycleBufs();
		}
	}

	public Exception getCloseError() {
		return closeError;
	}

	@Override
	public String toString() {
		return "HttpClientConnection{" +
				"callback=" + callback +
				", response=" + response +
				", httpClient=" + client +
				", keepAlive=" + (pool == client.poolKeepAlive) +
//				", lastRequestUrl='" + (request.getFullUrl() == null ? "" : request.getFullUrl()) + '\'' +
				", remoteAddress=" + remoteAddress +
				',' + super.toString() +
				'}';
	}
}
