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
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UnknownFormatException;
import io.datakernel.http.AsyncHttpClient.Inspector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.SP;
import static io.datakernel.bytebuf.ByteBufStrings.decodePositiveInt;
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
 *     "writing" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset/write timeout"]
 *     "reading" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset/read timeout"]
 *     "keep-alive" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset"]
 *     "taken(null)" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset"]
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
final class HttpClientConnection extends AbstractHttpConnection {
	public static final ParseException INVALID_RESPONSE = new UnknownFormatException(HttpClientConnection.class, "Invalid response");
	private SettablePromise<HttpResponse> promise;
	private HttpResponse response;
	private final AsyncHttpClient client;
	@Nullable
	private final Inspector inspector;

	final InetSocketAddress remoteAddress;
	HttpClientConnection addressPrev;
	HttpClientConnection addressNext;
	final int maxBodySize;

	HttpClientConnection(Eventloop eventloop, AsyncHttpClient client,
			AsyncTcpSocket asyncTcpSocket, InetSocketAddress remoteAddress) {
		super(eventloop, asyncTcpSocket);
		this.remoteAddress = remoteAddress;
		this.client = client;
		this.maxBodySize = client.maxBodySize;
		this.inspector = client.inspector;
	}

	@Override
	public void onClosedWithError(@NotNull Throwable e) {
		if (inspector != null) inspector.onHttpError(this, (flags & KEEP_ALIVE) != 0, e);
		if (promise != null) {
			SettablePromise<HttpResponse> promise = this.promise;
			this.promise = null;
			promise.setException(e);
		}
	}

	@Override
	protected void onStartLine(byte[] line, int limit) throws ParseException {
		boolean http1x = line[0] == 'H' && line[1] == 'T' && line[2] == 'T' && line[3] == 'P' && line[4] == '/' && line[5] == '1';
		boolean http11 = line[6] == '.' && line[7] == '1' && line[8] == SP;

		if (!http1x) throw INVALID_RESPONSE;

		int sp1;
		if (http11) {
			flags |= KEEP_ALIVE;
			sp1 = 9;
		} else if (line[6] == '.' && line[7] == '0' && line[8] == SP) {
			sp1 = 9;
		} else if (line[6] == SP) {
			sp1 = 7;
		} else {
			throw new ParseException(HttpClientConnection.class, "Invalid response: " + new String(line, 0, limit, ISO_8859_1));
		}

		int sp2;
		for (sp2 = sp1; sp2 < limit; sp2++) {
			if (line[sp2] == SP) {
				break;
			}
		}

		int statusCode = decodePositiveInt(line, sp1, sp2 - sp1);
		if (!(statusCode >= 100 && statusCode < 600)) {
			throw new UnknownFormatException(HttpClientConnection.class, "Invalid HTTP Status Code " + statusCode);
		}
		response = new HttpResponse(statusCode);
		response.maxBodySize = maxBodySize;
		/*
		  RFC 2616, section 4.4
		  1.Any response message which "MUST NOT" include a message-body (such as the 1xx, 204, and 304 responses and any response to a HEAD request) is always
		  terminated by the first empty line after the header fields, regardless of the entity-header fields present in the message.
		 */
		int messageCode = response.getCode();
		if ((messageCode >= 100 && messageCode < 200) || messageCode == 204 || messageCode == 304) {
			// Reset Content-Length for the case keep-alive connection
			contentLength = 0;
		}
	}

	@Override
	protected void onHeaderBuf(ByteBuf buf) {
		response.addHeaderBuf(buf);
	}

	@Override
	protected void onHeader(HttpHeader header, byte[] array, int off, int len) throws ParseException {
		assert response != null;
		if (response.headers.size() >= MAX_HEADERS) throw TOO_MANY_HEADERS;
		response.addHeader(header, array, off, len);
	}

	@Override
	protected void onHeadersReceived(ChannelSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();

		HttpResponse response = this.response;
		response.bodySupplier = bodySupplier;
		if (inspector != null) inspector.onHttpResponse(this, response);

		SettablePromise<HttpResponse> promise = this.promise;
		this.promise = null;
		promise.set(response);

		if ((response.flags & HttpMessage.ACCESSED_BODY_STREAM) == 0) {
			if (bodySupplier instanceof ChannelSuppliers.ChannelSupplierOfValue) {
				((ChannelSuppliers.ChannelSupplierOfValue<ByteBuf>) bodySupplier).getValue().recycle();
			} else {
				bodySupplier.streamTo(BUF_RECYCLER);
			}
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
	public Promise<HttpResponse> send(HttpRequest request) {
		SettablePromise<HttpResponse> promise = new SettablePromise<>();
		this.promise = promise;
		assert pool == null;
		(pool = client.poolReadWrite).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
		HttpHeaderValue connectionHeader = CONNECTION_KEEP_ALIVE_HEADER;
		if (client.maxKeepAliveRequests != -1) {
			if (++numberOfKeepAliveRequests >= client.maxKeepAliveRequests) {
				connectionHeader = CONNECTION_CLOSE_HEADER;
			}
		}
		request.addHeader(CONNECTION, connectionHeader);
		ByteBuf buf = renderHttpMessage(request);
		if (buf != null) {
			writeBuf(buf);
		} else {
			writeHttpMessageAsChunkedStream(request);
		}
		request.recycle();
		if (!isClosed()) {
			try {
				readHttpMessage();
			} catch (ParseException e) {
				closeWithError(e);
			}
		}
		return promise;
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
	protected void onClosed() {
		assert promise == null;
		if (pool == client.poolKeepAlive) {
			AddressLinkedList addresses = client.addresses.get(remoteAddress);
			addresses.removeNode(this);
			if (addresses.isEmpty()) {
				client.addresses.remove(remoteAddress);
			}
		}

		// pool will be null if socket was closed by the value just before connection.send() invocation
		// (eg. if connection was in open(null) or taken(null) states)
		pool.removeNode(this);
		pool = null;

		client.onConnectionClosed();
		if (response != null) {
			response.recycle();
			response = null;
		}
	}

	@Override
	public String toString() {
		return "HttpClientConnection{" +
				"promise=" + promise +
				", response=" + response +
				", httpClient=" + client +
				", keepAlive=" + (pool == client.poolKeepAlive) +
//				", lastRequestUrl='" + (request.getFullUrl() == null ? "" : request.getFullUrl()) + '\'' +
				", remoteAddress=" + remoteAddress +
				',' + super.toString() +
				'}';
	}
}
