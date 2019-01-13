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

import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.ConstantException;
import io.datakernel.exception.ParseException;
import io.datakernel.inspector.AbstractInspector;
import io.datakernel.inspector.BaseInspector;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Duration;

import static io.datakernel.http.AbstractHttpConnection.READ_TIMEOUT_ERROR;
import static io.datakernel.http.HttpHeaders.*;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings({"WeakerAccess", "unused", "UnusedReturnValue"})
public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	public static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(30);

	static final HttpExceptionFormatter DEFAULT_ERROR_FORMATTER = e -> {
		HttpResponse response;
		if (e instanceof HttpException) {
			int code = ((HttpException) e).getCode();
			response = HttpResponse.ofCode(code).withBody(e.getLocalizedMessage().getBytes(UTF_8));
		} else if (e instanceof ParseException) {
			response = HttpResponse.ofCode(400).withBody(e.getLocalizedMessage().getBytes(UTF_8));
		} else if (e instanceof ConstantException) {
			response = HttpResponse.ofCode(500).withBody(e.getLocalizedMessage().getBytes(UTF_8));
		} else {
			response = HttpResponse.ofCode(500);
		}
		return response
				.withHeader(CACHE_CONTROL, "no-store")
				.withHeader(PRAGMA, "no-cache")
				.withHeader(AGE, "0");
	};

	@NotNull
	private final AsyncServlet servlet;
	private final char[] charBuffer = new char[1024];
	@NotNull
	private HttpExceptionFormatter errorFormatter = DEFAULT_ERROR_FORMATTER;
	int keepAliveTimeoutMillis = (int) DEFAULT_KEEP_ALIVE.toMillis();
	int maxKeepAliveRequests = -1;
	private int readWriteTimeoutMillis = 0;

	final ConnectionsLinkedList poolKeepAlive = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolReadWrite = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolServing = new ConnectionsLinkedList();
	private int poolKeepAliveExpired;
	private int poolReadWriteExpired;

	@Nullable
	private ScheduledRunnable expiredConnectionsCheck;

	@Nullable
	Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onHttpError(InetAddress remoteAddress, Throwable e);

		void onHttpRequest(HttpRequest request);

		void onHttpResponse(HttpRequest request, HttpResponse httpResponse);

		void onServletException(HttpRequest request, Throwable e);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
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
	private AsyncHttpServer(@NotNull Eventloop eventloop, @NotNull AsyncServlet servlet) {
		super(eventloop);
		this.servlet = servlet;
	}

	public static AsyncHttpServer create(@NotNull Eventloop eventloop, @NotNull AsyncServlet servlet) {
		return new AsyncHttpServer(eventloop, servlet);
	}

	public AsyncHttpServer withKeepAliveTimeout(@NotNull Duration keepAliveTime) {
		this.keepAliveTimeoutMillis = (int) keepAliveTime.toMillis();
		return this;
	}

	public AsyncHttpServer withMaxKeepAliveRequests(int maxKeepAliveRequests) {
		this.maxKeepAliveRequests = maxKeepAliveRequests;
		return this;
	}

	public AsyncHttpServer withNoKeepAlive() {
		return withKeepAliveTimeout(Duration.ZERO);
	}

	public AsyncHttpServer withReadWriteTimeout(@NotNull Duration readTimeout) {
		this.readWriteTimeoutMillis = (int) readTimeout.toMillis();
		return this;
	}

	public AsyncHttpServer withHttpErrorFormatter(@NotNull HttpExceptionFormatter httpExceptionFormatter) {
		this.errorFormatter = httpExceptionFormatter;
		return this;
	}

	public Duration getKeepAliveTimeout() {
		return Duration.ofMillis(keepAliveTimeoutMillis);
	}

	public Duration getReadWriteTimeout() {
		return Duration.ofMillis(readWriteTimeoutMillis);
	}

	public AsyncHttpServer withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	// endregion

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.delayBackground(1000L, () -> {
			expiredConnectionsCheck = null;
			poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
			if (readWriteTimeoutMillis != 0)
				poolReadWriteExpired += poolReadWrite.closeExpiredConnections(eventloop.currentTimeMillis() - readWriteTimeoutMillis, READ_TIMEOUT_ERROR);
			if (getConnectionsCount() != 0)
				scheduleExpiredConnectionsCheck();
		});
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		assert eventloop.inEventloopThread();
		if (expiredConnectionsCheck == null)
			scheduleExpiredConnectionsCheck();
		HttpServerConnection connection = new HttpServerConnection(eventloop, remoteAddress, socket, this, servlet, charBuffer);
		connection.serve();
	}

	@Nullable
	private SettablePromise<Void> closePromise;

	void onConnectionClosed() {
		if (getConnectionsCount() == 0 && closePromise != null) {
			closePromise.set(null);
			closePromise = null;
		}
	}

	@Override
	protected void onClose(SettablePromise<Void> promise) {
		poolKeepAlive.closeAllConnections();
		keepAliveTimeoutMillis = 0;
		if (getConnectionsCount() == 0) {
			promise.set(null);
		} else {
			closePromise = promise;
		}
	}

	@JmxAttribute(description = "current number of connections", reducer = JmxReducerSum.class)
	public int getConnectionsCount() {
		return poolKeepAlive.size() + poolReadWrite.size() + poolServing.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveCount() {
		return poolKeepAlive.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadWriteCount() {
		return poolReadWrite.size();
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
	public int getConnectionsReadWriteExpired() {
		return poolReadWriteExpired;
	}

	HttpResponse formatHttpError(Throwable e) {
		return errorFormatter.formatException(e);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}

}
