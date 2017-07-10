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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.jmx.ValueStats;
import io.datakernel.util.MemSize;

import java.net.InetAddress;

import static io.datakernel.http.AbstractHttpConnection.*;

/**
 * A server which works asynchronously. An instance of {@code AsyncHttpServer}
 * can be created by calling {@link #create(Eventloop, AsyncServlet)} method
 * and providing an {@link Eventloop} instance and an implementation of
 * {@link AsyncServlet}.
 * <p>
 * The creation of asynchronous http server implies few steps:
 * <ol>
 *     <li>Create an {@code eventloop} for a server</li>
 *     <li>Create a {@code servlet}, which will respond to received request</li>
 *     <li>Create a {@code server} with these instances</li>
 * </ol>
 * For example, consider an {@code AsyncHttpServer}:
 * <pre><code>final {@link Eventloop Eventloop} eventloop = Eventloop.create();
 * final {@link AsyncServlet AsyncServlet} servlet = new AsyncServlet() {
 *    {@literal @}Override
 *     public void serve({@link HttpRequest HttpRequest} request, final {@link ResultCallback ResultCallback&lt;HttpResponse&gt;} callback) {
 *     	final HttpResponse response = HttpResponse.ok200().withBody(ByteBufStrings.encodeAscii("Hello, client!"));
 *     		eventloop.post(new Runnable() {
 *		   {@literal @}Override
 *  		    public void run() {
 *  		    System.out.println("Request body: " + request.getBody().toString());
 *     			callback.setResult(response);
 *     		    }
 *  		});
 * 	}
 * };
 * AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenPort(40000);
 * server.listen();
 * eventloop.run(); //eventloop runs in current thread
 * </code>
 * </pre>
 * Now server is ready for accepting requests and responding to clients with
 * <pre>"Hello, client!"</pre> message. It's easy to create a client for this
 * example using {@link AsyncHttpClient} or send a request with, for example,
 * {@link AsyncTcpSocketImpl}.
 */
public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	public static final long DEFAULT_KEEP_ALIVE_MILLIS = 30 * 1000L;

	private static final HttpExceptionFormatter DEFAULT_ERROR_FORMATTER = new HttpExceptionFormatter() {
		@Override
		public HttpResponse formatException(Exception e) {
			if (e instanceof HttpException) {
				HttpException httpException = (HttpException) e;
				return HttpResponse.ofCode(httpException.getCode()).withNoCache();
			}
			if (e instanceof ParseException) {
				return HttpResponse.ofCode(400).withNoCache();
			}
			return HttpResponse.ofCode(500).withNoCache();
		}
	};

	private final AsyncServlet servlet;
	private HttpExceptionFormatter errorFormatter = DEFAULT_ERROR_FORMATTER;
	private int maxHttpMessageSize = Integer.MAX_VALUE;
	int keepAliveTimeoutMillis = (int) DEFAULT_KEEP_ALIVE_MILLIS;
	private int readTimeoutMillis = 0;
	private int writeTimeoutMillis = 0;

	private int connectionsCount;
	final ConnectionsLinkedList poolKeepAlive = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolReading = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolWriting = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolServing = new ConnectionsLinkedList();
	private int poolKeepAliveExpired;
	private int poolReadingExpired;
	private int poolWritingExpired;

	private final char[] headerChars = new char[MAX_HEADER_LINE_SIZE];

	private AsyncCancellable expiredConnectionsCheck;

	Inspector inspector;

	public interface Inspector {
		void onHttpError(InetAddress remoteAddress, Exception e);

		void onHttpRequest(HttpRequest request);

		void onHttpResponse(HttpRequest request, HttpResponse httpResponse);

		void onServletException(HttpRequest request, Exception e);
	}

	public static class JmxInspector implements Inspector {
		private static final double SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;

		private final EventStats totalRequests = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats totalResponses = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats httpTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats httpErrors = ExceptionStats.create();
		private final ExceptionStats servletExceptions = ExceptionStats.create();

		@Override
		public void onHttpError(InetAddress remoteAddress, Exception e) {
			if (e == AbstractHttpConnection.READ_TIMEOUT_ERROR || e == AbstractHttpConnection.WRITE_TIMEOUT_ERROR) {
				httpTimeouts.recordEvent();
			} else {
				httpErrors.recordException(e);
			}
		}

		@Override
		public void onHttpRequest(HttpRequest request) {
			totalRequests.recordEvent();
		}

		@Override
		public void onHttpResponse(HttpRequest request, HttpResponse httpResponse) {
			totalResponses.recordEvent();
		}

		@Override
		public void onServletException(HttpRequest request, Exception e) {
			servletExceptions.recordException(e, request.toString());
		}

		@JmxAttribute(extraSubAttributes = "totalCount")
		public EventStats getTotalRequests() {
			return totalRequests;
		}

		@JmxAttribute(extraSubAttributes = "totalCount")
		public EventStats getTotalResponses() {
			return totalResponses;
		}

		@JmxAttribute
		public EventStats getHttpTimeouts() {
			return httpTimeouts;
		}

		@JmxAttribute(description = "Number of requests which were invalid according to http protocol. " +
				"Responses were not sent for this requests")
		public ExceptionStats getHttpErrors() {
			return httpErrors;
		}

		@JmxAttribute(description = "Number of requests which were valid according to http protocol, " +
				"but application produced error during handling this request " +
				"(responses with 4xx and 5xx HTTP status codes)")
		public ExceptionStats getServletExceptions() {
			return servletExceptions;
		}
	}

	// region builders
	private AsyncHttpServer(Eventloop eventloop, AsyncServlet servlet) {
		super(eventloop);
		this.servlet = servlet;
	}

	public static AsyncHttpServer create(Eventloop eventloop, AsyncServlet servlet) {
		return new AsyncHttpServer(eventloop, servlet).withInspector(new JmxInspector());
	}

	public AsyncHttpServer withKeepAliveTimeout(long keepAliveTimeMillis) {
		this.keepAliveTimeoutMillis = (int) keepAliveTimeMillis;
		return self();
	}

	public AsyncHttpServer withNoKeepAlive() {
		return withKeepAliveTimeout(0);
	}

	public AsyncHttpServer withReadTimeout(long readTimeoutMillis) {
		this.readTimeoutMillis = (int) readTimeoutMillis;
		return self();
	}

	public AsyncHttpServer withWriteTimeout(long writeTimeoutMillis) {
		this.writeTimeoutMillis = (int) writeTimeoutMillis;
		return self();
	}

	public AsyncHttpServer withMaxHttpMessageSize(int maxHttpMessageSize) {
		this.maxHttpMessageSize = maxHttpMessageSize;
		return self();
	}

	public AsyncHttpServer withMaxHttpMessageSize(MemSize size) {
		return withMaxHttpMessageSize((int) size.get());
	}

	public AsyncHttpServer withHttpErrorFormatter(HttpExceptionFormatter httpExceptionFormatter) {
		this.errorFormatter = httpExceptionFormatter;
		return self();
	}

	public AsyncHttpServer withInspector(Inspector inspector) {
		this.inspector = inspector;
		return self();
	}
	// endregion

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.scheduleBackground(eventloop.currentTimeMillis() + 1000L, new Runnable() {
			@Override
			public void run() {
				expiredConnectionsCheck = null;
				poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
				if (readTimeoutMillis != 0)
					poolReadingExpired += poolReading.closeExpiredConnections(eventloop.currentTimeMillis() - readTimeoutMillis, READ_TIMEOUT_ERROR);
				if (writeTimeoutMillis != 0)
					poolWritingExpired += poolWriting.closeExpiredConnections(eventloop.currentTimeMillis() - writeTimeoutMillis, WRITE_TIMEOUT_ERROR);
				if (connectionsCount != 0)
					scheduleExpiredConnectionsCheck();
			}
		});
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		assert eventloop.inEventloopThread();
		connectionsCount++;
		if (expiredConnectionsCheck == null)
			scheduleExpiredConnectionsCheck();
		return new HttpServerConnection(eventloop, asyncTcpSocket.getRemoteSocketAddress().getAddress(), asyncTcpSocket, this, servlet,
				headerChars, maxHttpMessageSize);
	}

	private CompletionCallback closeCallback;

	void onConnectionClosed() {
		connectionsCount--;
		if (connectionsCount == 0 && closeCallback != null) {
			closeCallback.postComplete(eventloop);
			closeCallback = null;
		}
	}

	@Override
	protected void onClose(final CompletionCallback completionCallback) {
		poolKeepAlive.closeAllConnections();
		keepAliveTimeoutMillis = 0;
		if (connectionsCount == 0) {
			completionCallback.postComplete(eventloop);
		} else {
			this.closeCallback = completionCallback;
		}
	}

	@JmxAttribute(description = "current number of connections", reducer = JmxReducerSum.class)
	public int getConnectionsCount() {
		return connectionsCount;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveCount() {
		return poolKeepAlive.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadingCount() {
		return poolReading.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsWritingCount() {
		return poolWriting.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsServingCount() {
		return poolServing.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveExpired() {
		return poolKeepAliveExpired;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadingExpired() {
		return poolReadingExpired;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsWritingExpired() {
		return poolWritingExpired;
	}

	HttpResponse formatHttpError(Exception e) {
		return errorFormatter.formatException(e);
	}

	@JmxAttribute(name = "")
	public JmxInspector getStats() {
		return inspector instanceof JmxInspector ? (JmxInspector) inspector : null;
	}
}
