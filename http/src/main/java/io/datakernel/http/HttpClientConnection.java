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

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.SP;
import static io.datakernel.bytebuf.ByteBufStrings.decodeDecimal;

@SuppressWarnings("ThrowableInstanceNeverThrown")
final class HttpClientConnection extends AbstractHttpConnection {
	private static final ParseException CLOSED_CONNECTION = new ParseException("Connection unexpectedly closed");

	private ResultCallback<HttpResponse> callback;
	private HttpResponse response;
	private final AsyncHttpClient httpClient;
	private final AsyncHttpClient.Inspector inspector;

	ExposedLinkedList<HttpClientConnection> keepAlivePoolByAddress;
	final ExposedLinkedList.Node<HttpClientConnection> keepAlivePoolByAddressNode = new ExposedLinkedList.Node<>(this);

	final InetSocketAddress remoteAddress;

	HttpClientConnection(Eventloop eventloop, InetSocketAddress remoteAddress,
	                     AsyncTcpSocket asyncTcpSocket, AsyncHttpClient httpClient, char[] headerChars,
	                     int maxHttpMessageSize) {
		super(eventloop, asyncTcpSocket, headerChars, maxHttpMessageSize);
		this.remoteAddress = remoteAddress;
		this.httpClient = httpClient;
		this.inspector = httpClient.inspector;
	}

	@Override
	public void onClosedWithError(Exception e) {
		assert eventloop.inEventloopThread();
		super.onClosedWithError(e);
		if (inspector != null) inspector.onConnectionException(this, callback == null, e);
		if (callback != null) {
			callback.postException(eventloop, e);
			callback = null;
		}
		onClosed();
	}

	@Override
	protected void onFirstLine(ByteBuf line) throws ParseException {
		if (line.peek(0) != 'H' || line.peek(1) != 'T' || line.peek(2) != 'T' || line.peek(3) != 'P' || line.peek(4) != '/' || line.peek(5) != '1') {
			line.recycle();
			throw new ParseException("Invalid response");
		}

		int sp1;
		if (line.peek(6) == SP) {
			sp1 = line.readPosition() + 7;
		} else if (line.peek(6) == '.' && (line.peek(7) == '1' || line.peek(7) == '0') && line.peek(8) == SP) {
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

		if (inspector != null) inspector.onConnectionResponse(this, response);
		if (keepAlive) {
			reset();
			returnToPool();
		} else {
			close();
		}
	}

	@Override
	public void onReadEndOfStream() {
		assert eventloop.inEventloopThread();
		if (callback != null) {
			if (reading == BODY && contentLength == UNKNOWN_LENGTH) {
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

	private void writeHttpRequest(HttpRequest httpRequest) {
		httpRequest.addHeader(keepAlive ? CONNECTION_KEEP_ALIVE_HEADER : CONNECTION_CLOSE_HEADER);
		asyncTcpSocket.write(httpRequest.toByteBuf());
	}

	/**
	 * Sends the request, recycles it and closes connection in case of timeout
	 *
	 * @param request  request for sending
	 * @param callback callback for handling result
	 */
	public void send(HttpRequest request, ResultCallback<HttpResponse> callback) {
		this.callback = callback;
		writeHttpRequest(request);

		request.recycleBufs();
	}

	@Override
	public void onWrite() {
		assert !isClosed();
		assert eventloop.inEventloopThread();
		reading = FIRSTLINE;
		asyncTcpSocket.read();
	}

	@Override
	protected final void returnToPool() {
		super.returnToPool();
		httpClient.returnToPool(this);
	}

	@Override
	protected final void removeFromPool() {
		super.removeFromPool();
		httpClient.removeFromPool(this);
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
	protected void onClosed() {
		assert callback == null;
		if (inspector != null) inspector.onConnectionClosed(this);
		super.onClosed();
		bodyQueue.clear();
		if (response != null) {
			response.recycleBufs();
		}
	}

	@Override
	public String toString() {
		return "HttpClientConnection{" +
				"callback=" + callback +
				", response=" + response +
				", httpClient=" + httpClient +
				", keepAlivePoolByAddress=" + (keepAlivePoolByAddress == null ? "" : keepAlivePoolByAddress) +
//				", lastRequestUrl='" + (request.getFullUrl() == null ? "" : request.getFullUrl()) + '\'' +
				", remoteAddress=" + remoteAddress +
				',' + super.toString() +
				'}';
	}
}
