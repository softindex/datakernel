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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.SerialSupplier;

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.SP;
import static io.datakernel.bytebuf.ByteBufStrings.decodeDecimal;
import static io.datakernel.http.HttpHeaders.CONNECTION;

/**
 * <p>
 * This class is responsible for sending and receiving HTTP requests.
 * It's made so that one instance of it corresponds to one networking socket.
 * That's why instances of those classes are all stored in one of three pools in their
 * respective {@link AsyncHttpClient} instance.
 * </p>
 * <p>
 * Those pools are: <code>poolKeepAlive</code>, <code>poolReading</code>, and <code>poolWriting</code>.
 * </p>
 * Path between those pools that any connection takes can be represented as a state machine,
 * described as a GraphViz graph below.
 * Nodes with (null) descriptor mean that the connection is not in any pool.
 * <pre>
 * digraph {
 *     label="Single HttpConnection state machine"
 *     rankdir="LR"
 *
 *     "open(null)"
 *     "closed(null)"
 *     "reading"
 *     "writing"
 *     "keep-alive"
 *     "taken(null)"
 *
 *     "writing" -> "closed(null)" [color="#ff8080", style=dashed, label="peer reset/write timeout"]
 *     "reading" -> "closed(null)" [color="#ff8080", style=dashed, label="peer reset/read timeout"]
 *     "keep-alive" -> "closed(null)" [color="#ff8080", style=dashed, label="peer reset"]
 *     "taken(null)" -> "closed(null)" [color="#ff8080", style=dashed, label="peer reset"]
 *
 *     "open(null)" -> "writing" [label="send request"]
 *     "writing" -> "reading"
 *     "reading" -> "closed(null)" [label="received response\n(no keep-alive)"]
 *     "reading" -> "keep-alive" [label="received response"]
 *     "keep-alive" -> "taken(null)" [label="reuse connection"]
 *     "taken(null)" -> "writing" [label="send request"]
 *     "keep-alive" -> "closed(null)" [label="expiration"]
 *
 *     { rank=same; "open(null)", "closed(null)" }
 *     { rank=same; "reading", "writing", "keep-alive" }
 * }
 * </pre>
 */
@SuppressWarnings("ThrowableInstanceNeverThrown")
final class HttpClientConnection extends AbstractHttpConnection {
	private static final HttpHeaders.Value CONNECTION_KEEP_ALIVE = HttpHeaders.asBytes(CONNECTION, "keep-alive");

	@Nullable
	private SettableStage<HttpResponse> result;
	@Nullable
	private HttpResponse response;
	private final AsyncHttpClient client;
	private final AsyncHttpClient.Inspector inspector;

	final InetSocketAddress remoteAddress;
	HttpClientConnection addressPrev;
	HttpClientConnection addressNext;

	HttpClientConnection(Eventloop eventloop, InetSocketAddress remoteAddress,
			AsyncTcpSocket asyncTcpSocket, AsyncHttpClient client, char[] headerChars) {
		super(eventloop, asyncTcpSocket, headerChars);
		this.remoteAddress = remoteAddress;
		this.client = client;
		this.inspector = client.inspector;
	}

	@Override
	public void onClosedWithError(Throwable e) {
		if (inspector != null) inspector.onHttpError(this, result == null, e);
		readQueue.recycle();
		if (result != null) {
			SettableStage<HttpResponse> callback = this.result;
			eventloop.post(() -> callback.setException(e));
			this.result = null;
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
			if (line.peek(7) == '1') {
				flags |= KEEP_ALIVE;
			}
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
		assert response != null;
		response.addHeader(header, value);
	}

	@Override
	protected void onHeadersReceived(SerialSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();

		assert response != null;
		response.bodySupplier = bodySupplier;

		if (inspector != null) inspector.onHttpResponse(this, response);

		assert result != null;
		result.set(response);
		result = null;
	}

	@Override
	protected void onBodyReceived() {
		if (response != null && (flags & (BODY_SENT | BODY_RECEIVED)) == (BODY_SENT | BODY_RECEIVED)) {
			onHttpMessageComplete();
	}
	}

	@Override
	protected void onBodySent() {
		if (response != null && (flags & (BODY_SENT | BODY_RECEIVED)) == (BODY_SENT | BODY_RECEIVED)) {
			onHttpMessageComplete();
	}
	}

	private void onHttpMessageComplete() {
		assert response != null;
		response.recycle();
		response = null;

		if ((flags & KEEP_ALIVE) != 0 && client.keepAliveTimeoutMillis != 0) {
			flags = 0;
			client.returnToKeepAlivePool(this);
		} else {
			close();
		}
	}

	/**
	 * Sends the request, recycles it and closes connection in case of timeout
	 *
	 * @param request request for sending
	 */
	public Stage<HttpResponse> send(HttpRequest request) {
		this.result = new SettableStage<>();
		switchPool(client.poolReadWrite);
		request.addHeader(CONNECTION_KEEP_ALIVE);
		writeHttpMessage(request);
		readHttpMessage();
		return this.result;
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
	protected void onClosed() {
		assert result == null;
		if (pool == client.poolKeepAlive) {
			AddressLinkedList addresses = client.addresses.get(remoteAddress);
			addresses.removeNode(this);
			if (addresses.isEmpty()) {
				client.addresses.remove(remoteAddress);
			}
		}

		// pool will be null if socket was closed by the peer just before connection.send() invocation
		// (eg. if connection was in open(null) or taken(null) states)
		switchPool(null);

		client.onConnectionClosed();
		if (response != null) {
			response.recycle();
			response = null;
		}
	}

	@Override
	public String toString() {
		return "HttpClientConnection{" +
				"callback=" + result +
				", response=" + response +
				", httpClient=" + client +
				", keepAlive=" + (pool == client.poolKeepAlive) +
//				", lastRequestUrl='" + (request.getFullUrl() == null ? "" : request.getFullUrl()) + '\'' +
				", remoteAddress=" + remoteAddress +
				',' + super.toString() +
				'}';
	}
}
