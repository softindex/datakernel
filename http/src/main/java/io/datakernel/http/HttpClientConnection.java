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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UnknownFormatException;
import io.datakernel.serial.SerialSupplier;

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.SP;
import static io.datakernel.bytebuf.ByteBufStrings.decodeDecimal;
import static io.datakernel.http.HttpHeaders.CONNECTION;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

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
	public static final ParseException INVALID_RESPONSE = new UnknownFormatException(HttpClientConnection.class, "Invalid response");
	private SettableStage<HttpResponse> callback;
	private HttpResponse response;
	private final AsyncHttpClient client;
	private final AsyncHttpClient.Inspector inspector;

	final InetSocketAddress remoteAddress;
	HttpClientConnection addressPrev;
	HttpClientConnection addressNext;

	HttpClientConnection(Eventloop eventloop, InetSocketAddress remoteAddress,
			AsyncTcpSocket asyncTcpSocket, AsyncHttpClient client) {
		super(eventloop, asyncTcpSocket);
		this.remoteAddress = remoteAddress;
		this.client = client;
		this.inspector = client.inspector;
	}

	@Override
	public void onClosedWithError(Throwable e) {
		if (inspector != null) inspector.onHttpError(this, callback == null, e);
		if (callback != null) {
			SettableStage<HttpResponse> callback = this.callback;
			this.callback = null;
			callback.setException(e);
		}
	}

	@Override
	protected void onFirstLine(byte[] line, int size) throws ParseException {
		assert line.length >= 16;
		if (line[0] != 'H' || line[1] != 'T' || line[2] != 'T' || line[3] != 'P' || line[4] != '/' || line[5] != '1') {
			throw INVALID_RESPONSE;
		}

		int sp1;
		if (line[6] == SP) {
			sp1 = 7;
		} else if (line[6] == '.' && (line[7] == '1' || line[7] == '0') && line[8] == SP) {
			if (line[7] == '1') {
				flags |= KEEP_ALIVE;
			}
			sp1 = 9;
		} else {
			throw new ParseException(HttpClientConnection.class, "Invalid response: " + new String(line, 0, size, ISO_8859_1));
		}

		int sp2;
		for (sp2 = sp1; sp2 < size; sp2++) {
			if (line[sp2] == SP) {
				break;
			}
		}

		int statusCode = decodeDecimal(line, sp1, sp2 - sp1);
		if (!(statusCode >= 100 && statusCode < 600)) {
			throw new UnknownFormatException(HttpClientConnection.class, "Invalid HTTP Status Code " + statusCode);
		}
		response = HttpResponse.ofCode(statusCode);
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

	@Override
	protected void onHeader(HttpHeader header, ByteBuf buf) throws ParseException {
		super.onHeader(header, buf);
		assert response != null;
		if (response.headers.size() >= MAX_HEADERS) throw TOO_MANY_HEADERS;
		response.addHeader(header, buf);
	}

	@Override
	protected void onHeadersReceived(ByteBuf body, SerialSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();
		assert body != null ^ bodySupplier != null;

		HttpResponse response = this.response;
		response.body = body;
		response.bodySupplier = bodySupplier;
		if (inspector != null) inspector.onHttpResponse(this, response);

		SettableStage<HttpResponse> callback = this.callback;
		this.callback = null;
		callback.set(response);

		if (response.body != null) {
			response.body.recycle();
			response.body = null;
		}
		if (response.bodySupplier != null) {
			response.bodySupplier.streamTo(BUF_RECYCLER);
			response.bodySupplier = null;
		}
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
			socket.read()
					.whenComplete((buf, e) -> {
						if (e == null) {
							if (buf != null) {
								closeWithError(UNEXPECTED_READ);
							} else {
								close();
							}
						} else {
							closeWithError(e);
						}
					});
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
		this.callback = new SettableStage<>();
		switchPool(client.poolReadWrite);
		HttpHeaderValue connectionHeader = CONNECTION_KEEP_ALIVE_HEADER;
		if (client.maxKeepAliveRequests != -1) {
			if (++numberOfKeepAliveRequests >= client.maxKeepAliveRequests) {
				connectionHeader = CONNECTION_CLOSE_HEADER;
			}
		}
		request.setHeader(CONNECTION, connectionHeader);
		writeHttpMessage(request);
		readHttpMessage();
		return this.callback;
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
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
