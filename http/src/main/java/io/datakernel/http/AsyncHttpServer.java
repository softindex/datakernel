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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.util.MemSize;

import java.net.InetAddress;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;

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
	private long keepAliveTimeMillis = DEFAULT_KEEP_ALIVE_MILLIS;
	private boolean gzipResponses = false;

	private final ExposedLinkedList<AbstractHttpConnection> keepAlivePool = ExposedLinkedList.create();
	private final char[] headerChars = new char[MAX_HEADER_LINE_SIZE];

	private AsyncCancellable expiredConnectionsCheck;

	private final EventStats connectionExpirations = EventStats.create();

	Inspector inspector;

	public interface Inspector extends AbstractServer.Inspector {
		void onConnectionError(InetAddress remoteAddress, Exception e);

		void onConnectionClosed(InetAddress remoteAddress);

		void onHttpRequest(HttpRequest request);

		void onHttpResponse(HttpRequest request, HttpResponse httpResponse);

		void onServletError(HttpRequest request, Exception e);
	}

	public static class JmxInspector extends AbstractServer.JmxInspector implements Inspector {
		private final EventStats totalRequests = EventStats.create();
		private final EventStats httpsRequests = EventStats.create();
		private final EventStats closed = EventStats.create();
		private final ExceptionStats errors = ExceptionStats.create();
		private final ExceptionStats applicationErrors = ExceptionStats.create();

		@Override
		public void onConnectionError(InetAddress remoteAddress, Exception e) {
			errors.recordException(e, remoteAddress);
		}

		@Override
		public void onConnectionClosed(InetAddress remoteAddress) {
			closed.recordEvent();
		}

		@Override
		public void onHttpRequest(HttpRequest request) {
			totalRequests.recordEvent();

			if (request.isHttps())
				httpsRequests.recordEvent();

//			if (keepAlive) {
//				keepAliveRequests.recordEvent();
//			} else {
//				nonKeepAliveRequests.recordEvent();
//			}

//			if (isMonitorCurrentRequestsHandlingDuration()) {
//				String url = request.getFullUrl();
//				long timestamp = eventloop.currentTimeMillis();
//				currentRequestHandlingStart.put(conn, new UrlWithTimestamp(url, timestamp));
//			}
		}

		@Override
		public void onHttpResponse(HttpRequest request, HttpResponse httpResponse) {
//			if (isMonitorCurrentRequestsHandlingDuration()) {
//				currentRequestHandlingStart.remove(conn);
//			}
		}

		@Override
		public void onServletError(HttpRequest request, Exception e) {
			applicationErrors.recordException(e, request);
		}

		@JmxAttribute()
		public EventStats getTotalRequests() {
			return totalRequests;
		}

		@JmxAttribute(description = "requests that was sent over secured connection (https)")
		public EventStats getHttpsRequests() {
			return httpsRequests;
		}

		@JmxAttribute(description = "Number of requests which were invalid according to http protocol. " +
				"Responses were not sent for this requests")
		public ExceptionStats getErrors() {
			return errors;
		}

		@JmxAttribute(description = "Number of requests which were valid according to http protocol, " +
				"but application produced error during handling this request " +
				"(responses with 4xx and 5xx HTTP status codes)")
		public ExceptionStats getApplicationErrors() {
			return applicationErrors;
		}

		@JmxAttribute(description = "number of \"close connection\" events)")
		public EventStats getClosed() {
			return closed;
		}

		@JmxAttribute(
				description = "current number of live connections (totally in pool and in use)",
				reducer = JmxReducers.JmxReducerSum.class)
		public long getConnectionsActive() {
			return getAccepts().getTotalCount() - closed.getTotalCount();
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

	public AsyncHttpServer withKeepAliveTimeMillis(long keepAliveTimeMillis) {
		this.keepAliveTimeMillis = keepAliveTimeMillis;
		return self();
	}

	public AsyncHttpServer withNoKeepAlive() {
		return withKeepAliveTimeMillis(0);
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

	public AsyncHttpServer withGzipResponses(boolean gzipResponses) {
		this.gzipResponses = gzipResponses;
		return self();
	}

	public AsyncHttpServer withInspector(Inspector inspector) {
		super.inspector = inspector;
		this.inspector = inspector;
		return self();
	}
	// endregion

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck =
				eventloop.scheduleBackground(eventloop.currentTimeMillis() + 1000L, new ScheduledRunnable() {
					@Override
					public void run() {
						expiredConnectionsCheck = null;
						checkExpiredConnections();
						if (!keepAlivePool.isEmpty()) {
							scheduleExpiredConnectionsCheck();
						}
					}
				});
	}

	private int checkExpiredConnections() {
		int count = 0;
		final long now = eventloop.currentTimeMillis();

		ExposedLinkedList.Node<AbstractHttpConnection> node = keepAlivePool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			long idleTime = now - connection.keepAliveTimestamp;
			if (idleTime > keepAliveTimeMillis) {
				connection.close(); // self removing from this pool
				count++;
			}
		}
		connectionExpirations.recordEvents(count);
		return count;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		assert eventloop.inEventloopThread();
		return new HttpServerConnection(eventloop, asyncTcpSocket.getRemoteSocketAddress().getAddress(), asyncTcpSocket, this, servlet,
				headerChars, maxHttpMessageSize, gzipResponses);
	}

	@Override
	protected void onClose(final CompletionCallback completionCallback) {
		ExposedLinkedList.Node<AbstractHttpConnection> node = keepAlivePool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			connection.close();
		}

		// TODO(vmykhalko): consider proper usage of completion callback
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				completionCallback.setComplete();
			}
		});
	}

	void returnToPool(final HttpServerConnection connection) {
		if (!isRunning() || keepAliveTimeMillis == 0) {
			eventloop.execute(new Runnable() {
				@Override
				public void run() {
					connection.close();
				}
			});
			return;
		}

		keepAlivePool.addLastNode(connection.poolNode);
		connection.keepAliveTimestamp = eventloop.currentTimeMillis();

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	void removeFromPool(HttpServerConnection connection) {
		keepAlivePool.removeNode(connection.poolNode);
		connection.keepAliveTimestamp = 0L;
	}

	// jmx

	@JmxAttribute(
			description = "current number of connections",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getConnectionsCount() {
		return keepAlivePool.size();
	}

	@JmxAttribute(description = "number of expired connections in keep-alive pool (after appropriate timeout)")
	public EventStats getConnectionExpirations() {
		return connectionExpirations;
	}

	@JmxAttribute(
			description = "current number of connections in pool",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public long getConnectionsInPool() {
		return keepAlivePool.size();
	}

	HttpResponse formatHttpError(Exception e) {
		return errorFormatter.formatException(e);
	}

	@JmxAttribute
	public JmxInspector getStats() {
		return inspector instanceof JmxInspector ? (JmxInspector) inspector : null;
	}
}
