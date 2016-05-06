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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.jmx.MBeanFormat;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.util.StringUtils.join;
import static java.util.Arrays.asList;

/**
 * A HttpServer is bound to an IP address and port number and listens for incoming connections
 * from clients on this address. A HttpServer is supported  {@link AsyncHttpServlet} that completes all responses asynchronously.
 */
public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	private static final long CHECK_PERIOD = 1000L;
	private static final long MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private final ExposedLinkedList<AbstractHttpConnection> connectionsList;
	private final Runnable expiredConnectionsTask = createExpiredConnectionsTask();

	private final AsyncHttpServlet servlet;

	private AsyncCancellable scheduleExpiredConnectionCheck;
	private final char[] headerChars;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	//JMX
	private final EventStats expiredConnections;

	/**
	 * Creates new instance of AsyncHttpServer
	 *
	 * @param eventloop eventloop in which will handle this connection
	 * @param servlet   servlet for handling requests
	 */
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

		// JMX
		this.expiredConnections = new EventStats(getSmoothingWindow());
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

			assert connection.getEventloop().inEventloopThread();
			long idleTime = now - connection.getActivityTime();
			if (idleTime > MAX_IDLE_CONNECTION_TIME) {
				connection.close(); // self removing from this pool
				count++;
			}
		}
		expiredConnections.recordEvents(count);
		return count;
	}

	/**
	 * Creates connection for this server
	 *
	 * @param socketChannel socket from new connection
	 * @return new connection
	 */
	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		assert eventloop.inEventloopThread();

		HttpServerConnection connection = new HttpServerConnection(eventloop, socketChannel, servlet, connectionsList, headerChars, maxHttpMessageSize);
		if (connectionsList.isEmpty())
			scheduleExpiredConnectionCheck();
		return connection;
	}

	/**
	 * Closes all connections from this server
	 */
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

			assert connection.getEventloop().inEventloopThread();
			connection.close();
		}
	}

	@Override
	protected void onListen() {
	}

	// JMX
	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public int getConnectionsCount() {
		return connectionsList.size();
	}

	@JmxAttribute
	public EventStats getExpiredConnections() {
		return expiredConnections;
	}

	@JmxAttribute
	public List<String> getConnections() {
		List<String> info = new ArrayList<>();
		info.add("RemoteSocketAddress,isRegistered,LifeTime,ActivityTime");

		ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			String connectionInfo = join(',', asList(connection.getRemoteSocketAddress(), connection.isRegistered(),
					MBeanFormat.formatPeriodAgo(connection.getLifeTime()),
					MBeanFormat.formatPeriodAgo(connection.getActivityTime())));

			info.add(connectionInfo);
		}

		return info;
	}

	@Override
	@JmxAttribute
	public void setSmoothingWindow(double smoothingWindow) {
		super.setSmoothingWindow(smoothingWindow);
		expiredConnections.setSmoothingWindow(smoothingWindow);
	}
}
