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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.util.MemSize;

import java.net.InetAddress;
import java.time.Duration;

import static io.datakernel.http.AbstractHttpConnection.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	public static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(30);

	private static final HttpExceptionFormatter DEFAULT_ERROR_FORMATTER = e -> {
		if (e instanceof HttpException) {
			HttpException httpException = (HttpException) e;
			HttpResponse response = HttpResponse.ofCode(httpException.getCode()).withNoCache();
			String msg = e.getLocalizedMessage();
			if (msg != null) {
				response.withBody(msg.getBytes(UTF_8));
			}
			return response;
		}
		if (e instanceof ParseException) {
			return HttpResponse.ofCode(400).withNoCache();
		}
		return HttpResponse.ofCode(500).withNoCache();
	};

	private final AsyncServlet servlet;
	private HttpExceptionFormatter errorFormatter = DEFAULT_ERROR_FORMATTER;
	private int maxHttpMessageSize = Integer.MAX_VALUE;
	int keepAliveTimeoutMillis = (int) DEFAULT_KEEP_ALIVE.toMillis();
	int maxKeepAliveRequests = -1;
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

	private final char[] headerChars = new char[MAX_HEADER_LINE_SIZE.toInt()];

	@Nullable
	private ScheduledRunnable expiredConnectionsCheck;

	Inspector inspector;

	public interface Inspector {
		void onHttpError(InetAddress remoteAddress, Throwable e);

		void onHttpRequest(HttpRequest request);

		void onHttpResponse(HttpRequest request, HttpResponse httpResponse);

		void onServletException(HttpRequest request, Throwable e);
	}

	public static class JmxInspector implements Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final EventStats totalRequests = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats totalResponses = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats httpTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats httpErrors = ExceptionStats.create();
		private final ExceptionStats servletExceptions = ExceptionStats.create();

		@Override
		public void onHttpError(InetAddress remoteAddress, Throwable e) {
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
		public void onServletException(HttpRequest request, Throwable e) {
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

	public AsyncHttpServer withKeepAliveTimeout(Duration keepAliveTime) {
		long keepAliveTimeMillis = keepAliveTime.toMillis();

		checkArgument(keepAliveTimeMillis >= 0, "Keep alive timeout should not be less than zero");

		this.keepAliveTimeoutMillis = (int) keepAliveTimeMillis;
		return this;
	}

	public AsyncHttpServer withMaxKeepAliveRequests(int maxKeepAliveRequests) {
		checkArgument(maxKeepAliveRequests >= 0, "Maximum number of requests per keep-alive connection should not be less than zero");
		this.maxKeepAliveRequests = maxKeepAliveRequests;
		return this;
	}

	public AsyncHttpServer withNoKeepAlive() {
		return withKeepAliveTimeout(Duration.ZERO);
	}

	public AsyncHttpServer withReadTimeout(Duration readTimeout) {
		long readTimeoutMillis = readTimeout.toMillis();

		checkArgument(readTimeoutMillis >= 0, "Read timeout should not be less than zero");

		this.readTimeoutMillis = (int) readTimeoutMillis;
		return this;
	}

	public AsyncHttpServer withWriteTimeout(Duration writeTimeout) {
		long writeTimeoutMillis = writeTimeout.toMillis();

		checkArgument(writeTimeoutMillis >= 0, "Write timeout should not be less than zero");

		this.writeTimeoutMillis = (int) writeTimeoutMillis;
		return this;
	}

	public AsyncHttpServer withMaxHttpMessageSize(@Nullable MemSize maxHttpMessageSize) {
		this.maxHttpMessageSize = maxHttpMessageSize != null ? maxHttpMessageSize.toInt() : Integer.MAX_VALUE;
		return this;
	}

	public AsyncHttpServer withHttpErrorFormatter(HttpExceptionFormatter httpExceptionFormatter) {
		this.errorFormatter = httpExceptionFormatter;
		return this;
	}

	public AsyncHttpServer withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	public MemSize getMaxHttpMessageSize() {
		return MemSize.of(maxHttpMessageSize);
	}

	public Duration getKeepAliveTimeout() {
		return Duration.ofMillis(keepAliveTimeoutMillis);
	}

	public Duration getReadTimeout() {
		return Duration.ofMillis(readTimeoutMillis);
	}

	public Duration getWriteTimeout() {
		return Duration.ofMillis(writeTimeoutMillis);
	}

	// endregion

	@SuppressWarnings("Duplicates")
	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.delayBackground(1000L, () -> {
			expiredConnectionsCheck = null;
			poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
			if (readTimeoutMillis != 0)
				poolReadingExpired += poolReading.closeExpiredConnections(eventloop.currentTimeMillis() - readTimeoutMillis, READ_TIMEOUT_ERROR);
			if (writeTimeoutMillis != 0)
				poolWritingExpired += poolWriting.closeExpiredConnections(eventloop.currentTimeMillis() - writeTimeoutMillis, WRITE_TIMEOUT_ERROR);
			if (connectionsCount != 0)
				scheduleExpiredConnectionsCheck();
		});
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		assert eventloop.inEventloopThread();
		connectionsCount++;
		if (expiredConnectionsCheck == null)
			scheduleExpiredConnectionsCheck();
		return new HttpServerConnection(eventloop, asyncTcpSocket.getRemoteSocketAddress().getAddress(), asyncTcpSocket, this, servlet,
				headerChars);
	}

	@Nullable
	private SettableStage<Void> closeStage;

	void onConnectionClosed() {
		connectionsCount--;
		if (connectionsCount == 0 && closeStage != null) {
			closeStage.set(null);
			closeStage = null;
		}
	}

	@Override
	protected void onClose(SettableStage<Void> stage) {
		poolKeepAlive.closeAllConnections();
		keepAliveTimeoutMillis = 0;
		if (connectionsCount == 0) {
			stage.set(null);
		} else {
			this.closeStage = stage;
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

	HttpResponse formatHttpError(Throwable e) {
		return errorFormatter.formatException(e);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return inspector instanceof JmxInspector ? (JmxInspector) inspector : null;
	}

}
