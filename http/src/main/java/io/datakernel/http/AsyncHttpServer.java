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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;

public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	private static final long CHECK_PERIOD = 1000L;
	private static final long DEFAULT_MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private final ExposedLinkedList<AbstractHttpConnection> pool;

	private final AsyncHttpServlet servlet;

	private AsyncCancellable expiredConnectionsCheck;
	private final char[] headerChars;
	private int maxHttpMessageSize = Integer.MAX_VALUE;
	private long maxIdleConnectionTime = DEFAULT_MAX_IDLE_CONNECTION_TIME;

	// jmx
	private final EventStats totalRequests = new EventStats();
	private final EventStats httpsRequests = new EventStats();
	private final EventStats httpRequests = new EventStats();
	private final EventStats keepAliveRequests = new EventStats();
	private final EventStats nonKeepAliveRequests = new EventStats();
	private final EventStats expiredConnections = new EventStats();
	private final EventStats httpProtocolErrors = new EventStats();
	private final EventStats applicationErrors = new EventStats();

	public AsyncHttpServer(Eventloop eventloop, AsyncHttpServlet servlet) {
		super(eventloop);
		this.pool = new ExposedLinkedList<>();
		this.servlet = servlet;
		char[] chars = eventloop.get(char[].class);
		if (chars == null || chars.length < MAX_HEADER_LINE_SIZE) {
			chars = new char[MAX_HEADER_LINE_SIZE];
			eventloop.set(char[].class, chars);
		}
		this.headerChars = chars;
	}

	public AsyncHttpServer setMaxIdleConnectionTime(long maxIdleConnectionTime) {
		this.maxIdleConnectionTime = maxIdleConnectionTime;
		return this;
	}

	public AsyncHttpServer setMaxHttpMessageSize(int size) {
		this.maxHttpMessageSize = size;
		return this;
	}

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.scheduleBackground(eventloop.currentTimeMillis() + CHECK_PERIOD, new Runnable() {
			@Override
			public void run() {
				expiredConnectionsCheck = null;
				checkExpiredConnections();
				if (!pool.isEmpty()) {
					scheduleExpiredConnectionsCheck();
				}
			}
		});
	}

	private int checkExpiredConnections() {
		int count = 0;
		final long now = eventloop.currentTimeMillis();

		ExposedLinkedList.Node<AbstractHttpConnection> node = pool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			long idleTime = now - connection.poolTimestamp;
			if (idleTime > maxIdleConnectionTime) {
				connection.close(); // self removing from this pool
				count++;
			}
		}
		expiredConnections.recordEvents(count);
		return count;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		assert eventloop.inEventloopThread();
		return new HttpServerConnection(
				eventloop, asyncTcpSocket.getRemoteSocketAddress().getAddress(), asyncTcpSocket,
				this, servlet, pool, headerChars, maxHttpMessageSize);
	}

	@Override
	protected void onClose() {
		ExposedLinkedList.Node<AbstractHttpConnection> node = pool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			connection.close();
		}
	}

	void addToPool(HttpServerConnection connection) {
		pool.addLastNode(connection.poolNode);
		connection.poolTimestamp = eventloop.currentTimeMillis();

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	void removeFromPool(HttpServerConnection connection) {
		pool.removeNode(connection.poolNode);
		connection.poolTimestamp = 0L;
	}

	// jmx
	void recordRequestEvent(boolean https, boolean keepAlive) {
		totalRequests.recordEvent();

		if (https) {
			httpsRequests.recordEvent();
		} else {
			httpRequests.recordEvent();
		}

		if (keepAlive) {
			keepAliveRequests.recordEvent();
		} else {
			nonKeepAliveRequests.recordEvent();
		}
	}

	void recordHttpProtocolError() {
		httpProtocolErrors.recordEvent();
	}

	void recordApplicationError() {
		applicationErrors.recordEvent();
	}

	@JmxAttribute(
			description = "current number of connections",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getConnectionsCount() {
		return pool.size();
	}

	@JmxAttribute()
	public EventStats getTotalRequests() {
		return totalRequests;
	}

	@JmxAttribute(description = "requests that was sent over secured connection (https)")
	public EventStats getHttpsRequests() {
		return httpsRequests;
	}

	@JmxAttribute(description = "requests that was sent over unsecured connection (http)")
	public EventStats getHttpRequests() {
		return httpRequests;
	}

	@JmxAttribute(description = "after handling this type of request connection is returned to pool")
	public EventStats getKeepAliveRequests() {
		return keepAliveRequests;
	}

	@JmxAttribute(description = "after handling this type of request connection is closed")
	public EventStats getNonKeepAliveRequests() {
		return nonKeepAliveRequests;
	}

	@JmxAttribute(description = "number of expired connections in pool (after appropriate timeout)")
	public EventStats getExpiredConnections() {
		return expiredConnections;
	}

	@JmxAttribute(description = "Number of requests which were invalid according to http protocol. " +
			"Responses were not sent for this requests")
	public EventStats getHttpProtocolErrors() {
		return httpProtocolErrors;
	}

	@JmxAttribute(description = "Number of requests which were valid according to http protocol, " +
			"but application produced error during handling this request " +
			"(responses with 4xx and 5xx HTTP status codes)")
	public EventStats getApplicationErrors() {
		return applicationErrors;
	}
}
