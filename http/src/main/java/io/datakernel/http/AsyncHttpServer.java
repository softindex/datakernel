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
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.util.Preconditions.check;

public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	private static final long CHECK_PERIOD = 1000L;
	private static final long DEFAULT_MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private final ExposedLinkedList<AbstractHttpConnection> connectionsList;
	private final Runnable expiredConnectionsTask = createExpiredConnectionsTask();

	private final AsyncHttpServlet servlet;

	private AsyncCancellable scheduleExpiredConnectionCheck;
	private final char[] headerChars;
	private int maxHttpMessageSize = Integer.MAX_VALUE;
	private long maxIdleConnectionTime = DEFAULT_MAX_IDLE_CONNECTION_TIME;

	public AsyncHttpServer(Eventloop eventloop, AsyncHttpServlet servlet) {
		super(eventloop);
		this.connectionsList = new ExposedLinkedList<>();
		this.servlet = servlet;
		char[] chars = eventloop.get(char[].class);
		if (chars == null || chars.length < MAX_HEADER_LINE_SIZE) {
			chars = new char[MAX_HEADER_LINE_SIZE];
			eventloop.set(char[].class, chars);
		}
		this.headerChars = chars;
	}

	public AsyncHttpServer setMaxIdleConnectionTime(long maxIdleConnectionTime) {
		check(maxIdleConnectionTime > 0);
		this.maxIdleConnectionTime = maxIdleConnectionTime;
		return this;
	}

	public AsyncHttpServer setMaxHttpMessageSize(int size) {
		this.maxHttpMessageSize = size;
		return this;
	}

	private Runnable createExpiredConnectionsTask() {
		return new Runnable() {
			@Override
			public void run() {
				checkExpiredConnections();
				if (!connectionsList.isEmpty())
					scheduleExpiredConnectionCheck();
			}
		};
	}

	private void scheduleExpiredConnectionCheck() {
		scheduleExpiredConnectionCheck = eventloop.schedule(eventloop.currentTimeMillis() + CHECK_PERIOD, expiredConnectionsTask);
	}

	private int checkExpiredConnections() {
		scheduleExpiredConnectionCheck = null;
		int count = 0;
		final long now = eventloop.currentTimeMillis();

		ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			long idleTime = now - connection.getLastUsedTime();
			if (idleTime > maxIdleConnectionTime) {
				connection.close(); // self removing from this pool
				count++;
			}
		}
		return count;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		assert eventloop.inEventloopThread();
		if (connectionsList.isEmpty()) scheduleExpiredConnectionCheck();
		return new HttpServerConnection(
				eventloop, asyncTcpSocket.getRemoteSocketAddress().getAddress(), asyncTcpSocket,
				servlet, connectionsList, headerChars, maxHttpMessageSize);
	}

	@Override
	protected void onClose() {
		closeConnections();
	}

	private void closeConnections() {
		if (scheduleExpiredConnectionCheck != null)
			scheduleExpiredConnectionCheck.cancel();

		ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			connection.close();
		}
	}

	@Override
	protected void onListen() {
	}

	// jmx
	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsCount() {
		return connectionsList.size();
	}
}
