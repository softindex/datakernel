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
import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.ExceptionCallback;
import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncSslSocket;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

import static io.datakernel.http.HttpHeaders.CONNECTION;
import static io.datakernel.util.ByteBufStrings.SP;
import static io.datakernel.util.ByteBufStrings.decodeDecimal;

@SuppressWarnings("ThrowableInstanceNeverThrown")
final class HttpClientConnection extends AbstractHttpConnection {
	private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();
	private static final ParseException CLOSED_CONNECTION = new ParseException("Connection unexpectedly closed");

	private static final HttpHeaders.Value CONNECTION_KEEP_ALIVE = HttpHeaders.asBytes(CONNECTION, "keep-alive");

	private ResultCallback<HttpResponse> callback;
	private AsyncCancellable timeoutCheck;
	private HttpResponse response;
	private final AsyncHttpClient httpClient;

	private final InetSocketAddress remoteSocketAddress;

	// creators
	HttpClientConnection(Eventloop eventloop, InetSocketAddress remoteSocketAddress,
	                     AsyncTcpSocket asyncTcpSocket, AsyncHttpClient httpClient,
	                     ExposedLinkedList<AbstractHttpConnection> pool,
	                     char[] headerChars, int maxHttpMessageSize) {
		super(eventloop, asyncTcpSocket, pool, headerChars, maxHttpMessageSize);
		this.remoteSocketAddress = remoteSocketAddress;
		this.httpClient = httpClient;
	}

	// miscellaneous
	@Override
	protected void reset() {
		reading = END_OF_STREAM;
		if (response != null) {
			response.recycleBufs();
			response = null;
		}
		if (timeoutCheck != null) {
			timeoutCheck.cancel();
			timeoutCheck = null;
		}
		super.reset();
	}

	// close methods
	@Override
	public void onClosedWithError(Exception e) {
		assert eventloop.inEventloopThread();
		onClosed();
		if (this.callback != null) {
			ResultCallback<HttpResponse> callback = this.callback;
			this.callback = null;
			callback.onException(e);
		}
		httpClient.connectsMonitor.onException(remoteSocketAddress, e);
	}

	@Override
	protected void cleanUpPool() {
		if (connectionNode != null) {
			removeConnectionFromPool();
			httpClient.removeFromCache(remoteSocketAddress, this);
			connectionNode = null;
		} else {
			httpClient.connectsMonitor.removeActive(remoteSocketAddress);
		}
	}

	@Override
	protected void onClosed() {
		if (isClosed()) return;
		super.onClosed();
		bodyQueue.clear();
		if (response != null) {
			response.recycleBufs();
			response = null;
		}
	}

	// read methods
	@Override
	protected void onFirstLine(ByteBuf line) throws ParseException {
		if (line.peek(0) != 'H' || line.peek(1) != 'T' || line.peek(2) != 'T' || line.peek(3) != 'P' || line.peek(4) != '/' || line.peek(5) != '1')
			throw new ParseException("Invalid response");

		int sp1;
		if (line.peek(6) == SP) {
			sp1 = line.getReadPosition() + 7;
		} else if (line.peek(6) == '.' && (line.peek(7) == '1' || line.peek(7) == '0') && line.peek(8) == SP) {
			sp1 = line.getReadPosition() + 9;
		} else
			throw new ParseException("Invalid response: " + new String(line.array(), line.getReadPosition(), line.remainingToRead()));

		int sp2;
		for (sp2 = sp1; sp2 < line.getWritePosition(); sp2++) {
			if (line.at(sp2) == SP) {
				break;
			}
		}

		int statusCode = decodeDecimal(line.array(), sp1, sp2 - sp1);
		if (!(statusCode >= 100 && statusCode < 600))
			throw new ParseException("Invalid HTTP Status Code " + statusCode);
		response = HttpResponse.create(statusCode);
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
	protected void onHeader(HttpHeader header, ByteBuf value) throws ParseException {
		super.onHeader(header, value);
		response.addHeader(header, value);
	}

	@Override
	protected void onHttpMessage(ByteBuf bodyBuf) {
		assert !isClosed();
		response.body(bodyBuf);
		if (callback != null) {
			ResultCallback<HttpResponse> callback = this.callback;
			this.callback = null;
			callback.onResult(response);
		}
		if (isClosed()) return;

		if (keepAlive) {
			reset();
			moveConnectionToPool();
			httpClient.moveToCache(remoteSocketAddress, this);
		} else {
			close();
		}
	}

	@Override
	public void onShutdownInput() {
		assert eventloop.inEventloopThread();
		if (callback != null) {
			if (reading == BODY && contentLength == UNKNOWN_LENGTH) {
				onHttpMessage(bodyQueue.takeRemaining());
			} else {
				closeWithError(CLOSED_CONNECTION);
			}
		} else {
			close();
		}
	}

	// write methods
	public void send(HttpRequest request, long timeout, ResultCallback<HttpResponse> callback) {
		this.callback = callback;
		writeHttpRequest(request);
		request.recycleBufs();
		scheduleTimeout(timeout);
	}

	private void writeHttpRequest(HttpRequest httpRequest) {
		if (keepAlive) {
			httpRequest.addHeader(CONNECTION_KEEP_ALIVE);
		}
		asyncTcpSocket.write(httpRequest.write());
	}

	@Override
	public void onWrite() {
		assert !isClosed();
		assert eventloop.inEventloopThread();
		reading = FIRSTCHAR;
		asyncTcpSocket.read();
	}

	private void scheduleTimeout(final long timeoutTime) {
		if (isClosed()) return;
		timeoutCheck = eventloop.scheduleBackground(timeoutTime, new Runnable() {
			@Override
			public void run() {
				closeWithError(TIMEOUT_EXCEPTION);
				if (!isClosed()) {
					// expect this connection to close either due to the socket r/w timeouts
					// or due to exceeding maxHttpMessageSize
					ExceptionCallback exceptionCallback = callback;
					callback = null;
					exceptionCallback.onException(TIMEOUT_EXCEPTION);
				}
			}
		});
	}

	@Nullable
	InetSocketAddress getRemoteSocketAddress() {
		return remoteSocketAddress;
	}

	boolean isSslConnection() {
		return this.asyncTcpSocket instanceof AsyncSslSocket;
	}
}
