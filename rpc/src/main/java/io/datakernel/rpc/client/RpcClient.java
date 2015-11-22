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

package io.datakernel.rpc.client;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.jmx.MBeanFormat;
import io.datakernel.jmx.MBeanUtils;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.RpcClientConnection.StatusListener;
import io.datakernel.rpc.client.sender.RpcNoSenderAvailableException;
import io.datakernel.rpc.client.sender.RpcRequestSender;
import io.datakernel.rpc.client.sender.RpcRequestSendingStrategy;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.protocol.RpcSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcClient implements NioService, RpcClientMBean {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);
	public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
	public static final int DEFAULT_RECONNECT_INTERVAL = 1 * 1000;

	private Logger logger = LoggerFactory.getLogger(RpcClient.class);

	private final NioEventloop eventloop;
	private RpcRequestSendingStrategy requestSendingStrategy;
	private List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private final RpcSerializer serializerFactory;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT;
	private int reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL;

	private RpcRequestSender requestSender;

	private CompletionCallback startCallback;
	private boolean running;

	// JMX
	private boolean monitoring;

	private final LastExceptionCounter lastException = new LastExceptionCounter("LastException");
	private int successfulConnects = 0;
	private int failedConnects = 0;
	private int closedConnects = 0;

	private final RpcClientConnectionPool pool = new RpcClientConnectionPool() {
		@Override
		public RpcClientConnection get(InetSocketAddress key) {
			return connections.get(key);
		}
	};

	private RpcClient(NioEventloop eventloop, RpcSerializer serializerFactory) {
		this.eventloop = eventloop;
		this.serializerFactory = serializerFactory;
	}

	public static RpcClient create(final NioEventloop eventloop, final RpcSerializer serializerFactory) {
		return new RpcClient(eventloop, serializerFactory);
	}

	public RpcClient strategy(RpcRequestSendingStrategy requestSendingStrategy) {
		this.requestSendingStrategy = requestSendingStrategy;
		this.addresses = new ArrayList<>(requestSendingStrategy.getAddresses());
		return this;
	}

	public RpcClient protocol(RpcProtocolFactory protocolFactory) {
		this.protocolFactory = protocolFactory;
		return this;
	}

	public RpcClient socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return this;
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	public RpcClient parentLogger(Logger parentLogger) {
		checkNotNull(parentLogger, "Logger must not be null");
		this.logger = LoggerFactory.getLogger(parentLogger.getName() + "$" + RpcClient.class.getSimpleName());
		return this;
	}

	public RpcClient connectTimeoutMillis(int connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
		return this;
	}

	public RpcClient reconnectIntervalMillis(int reconnectIntervalMillis) {
		this.reconnectIntervalMillis = reconnectIntervalMillis;
		return this;
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		checkNotNull(callback);
		checkState(!running);
		running = true;
		startCallback = callback;
		if (connectTimeoutMillis != 0) {
			eventloop.scheduleBackground(eventloop.currentTimeMillis() + connectTimeoutMillis, new Runnable() {
				@Override
				public void run() {
					if (running && startCallback != null) {
						String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
								connectTimeoutMillis / 1000.0);
						startCallback.onException(new InterruptedException(errorMsg));
						running = false;
						startCallback = null;
					}
				}
			});
		}

		for (InetSocketAddress address : addresses) {
			connect(address);
		}
	}

	public void stop() {
		checkState(eventloop.inEventloopThread());
		checkState(running);
		running = false;
		if (startCallback != null) {
			startCallback.onException(new InterruptedException("Start aborted"));
			startCallback = null;
		}
		closeConnections();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		stop();
		callback.onComplete();
	}

	private void connect(final InetSocketAddress address) {
		if (!running) {
			return;
		}

		logger.info("Connecting {}", address);
		eventloop.connect(address, socketSettings, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				StatusListener statusListener = new StatusListener() {
					@Override
					public void onOpen(RpcClientConnection connection) {
						addConnection(address, connection);
					}

					@Override
					public void onClosed() {
						logger.info("Connection to {} closed", address);
						removeConnection(address);
						closedConnects++;
						connect(address);
					}
				};
				RpcClientConnection connection = new RpcClientConnectionImpl(eventloop, socketChannel,
						serializerFactory.createSerializer(), protocolFactory, statusListener);
				connection.getSocketConnection().register();
				successfulConnects++;
				logger.info("Connection to {} established", address);
				if (startCallback != null) {
					startCallback.onComplete();
					startCallback = null;
				}
			}

			@Override
			public void onException(Exception exception) {
				lastException.update(exception, "Connect fail to " + address.toString(), eventloop.currentTimeMillis());
				failedConnects++;
				if (running) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, exception.toString());
					}
					eventloop.scheduleBackground(eventloop.currentTimeMillis() + reconnectIntervalMillis, new Runnable() {
						@Override
						public void run() {
							if (running) {
								connect(address);
							}
						}
					});
				}
			}
		});
	}

	private void addConnection(InetSocketAddress address, RpcClientConnection connection) {
		connections.put(address, connection);
		if (monitoring) {
			if (connection instanceof RpcClientConnectionMBean)
				((RpcClientConnectionMBean) connection).startMonitoring();
		}
		RpcRequestSender sender = requestSendingStrategy.createSender(pool);
		requestSender = sender != null ? sender : new RequestSenderError();
	}

	private void removeConnection(InetSocketAddress address) {
		connections.remove(address);
		RpcRequestSender sender = requestSendingStrategy.createSender(pool);
		requestSender = sender != null ? sender : new RequestSenderError();
	}

	private void closeConnections() {
		for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
			connection.close();
		}
	}

	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		requestSender.sendRequest(request, timeout, callback);
	}

	public <T> ResultCallbackFuture<T> sendRequestFuture(final Object request, final int timeout) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				sendRequest(request, timeout, future);
			}
		});
		return future;
	}

	// visible for testing
	public RpcRequestSender getRequestSender() {
		return requestSender;
	}

	private static final class RequestSenderError implements RpcRequestSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No senders available");

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}
	}

	// JMX
	public void registerMBean(MBeanServer mbeanServer, String domain, String serviceName, String clientName) {
		MBeanUtils.register(mbeanServer, MBeanFormat.name(domain, serviceName, clientName + "." + RpcClient.class.getSimpleName()), this);
		MBeanUtils.register(mbeanServer, MBeanFormat.name(domain, serviceName, clientName + "." + RpcClientConnectionPool.class.getSimpleName()), connections);
	}

	public void unregisterMBean(MBeanServer mbeanServer, String domain, String serviceName, String clientName) {
		MBeanUtils.unregisterIfExists(mbeanServer, MBeanFormat.name(domain, serviceName, clientName + "." + RpcClient.class.getSimpleName()));
		MBeanUtils.unregisterIfExists(mbeanServer, MBeanFormat.name(domain, serviceName, clientName + "." + RpcClientConnectionPool.class.getSimpleName()));
	}

	@Override
	public void startMonitoring() {
		monitoring = true;
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				((RpcClientConnectionMBean) connection).startMonitoring();
			}
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				((RpcClientConnectionMBean) connection).stopMonitoring();
			}
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		successfulConnects = 0;
		failedConnects = 0;
		closedConnects = 0;
		lastException.reset();
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				((RpcClientConnectionMBean) connection).reset();
			}
		}
	}

	@Override
	public String getAddresses() {
		return addresses.toString();
	}

	@Override
	public int getSuccessfulConnects() {
		return successfulConnects;
	}

	@Override
	public int getFailedConnects() {
		return failedConnects;
	}

	@Override
	public int getClosedConnects() {
		return closedConnects;
	}

	@Override
	public CompositeData getLastException() {
		return lastException.compositeData();
	}

	@Override
	public int getConnectionsCount() {
		return connections.size();
	}

	@Override
	public CompositeData[] getConnections() throws OpenDataException {
		List<CompositeData> compositeData = new ArrayList<>();
		for (Map.Entry<InetSocketAddress, RpcClientConnection> entry : connections.entrySet()) {
			InetSocketAddress address = entry.getKey();
			RpcClientConnection connection = entry.getValue();

			if (!(connection instanceof RpcClientConnectionMBean))
				continue;
			RpcClientConnectionMBean connectionMBean = (RpcClientConnectionMBean) connection;

			CompositeData lastTimeoutException = connectionMBean.getLastTimeoutException();
			CompositeData lastRemoteException = connectionMBean.getLastRemoteException();
			CompositeData lastProtocolException = connectionMBean.getLastProtocolException();
			CompositeData connectionDetails = connectionMBean.getConnectionDetails();

			compositeData.add(CompositeDataBuilder.builder("Rpc connections", "Rpc connections status")
					.add("Address", SimpleType.STRING, address.toString())
					.add("SuccessfulRequests", SimpleType.INTEGER, connectionMBean.getSuccessfulRequests())
					.add("FailedRequests", SimpleType.INTEGER, connectionMBean.getFailedRequests())
					.add("RejectedRequests", SimpleType.INTEGER, connectionMBean.getRejectedRequests())
					.add("ExpiredRequests", SimpleType.INTEGER, connectionMBean.getExpiredRequests())
					.add("PendingRequests", SimpleType.STRING, connectionMBean.getPendingRequestsStats())
					.add("ProcessResultTimeMicros", SimpleType.STRING, connectionMBean.getProcessResultTimeStats())
					.add("ProcessExceptionTimeMicros", SimpleType.STRING, connectionMBean.getProcessExceptionTimeStats())
					.add("SendPacketTimeMicros", SimpleType.STRING, connectionMBean.getSendPacketTimeStats())
					.add("LastTimeoutException", lastTimeoutException)
					.add("LastRemoteException", lastRemoteException)
					.add("LastProtocolException", lastProtocolException)
					.add("ConnectionDetails", connectionDetails)
					.build());
		}
		return compositeData.toArray(new CompositeData[compositeData.size()]);
	}

	@Override
	public long getTotalSuccessfulRequests() {
		long result = 0;
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				result += ((RpcClientConnectionMBean) connection).getSuccessfulRequests();
			}
		}
		return result;
	}

	@Override
	public long getTotalPendingRequests() {
		long result = 0;
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				result += ((RpcClientConnectionMBean) connection).getPendingRequests();
			}
		}
		return result;
	}

	@Override
	public long getTotalRejectedRequests() {
		long result = 0;
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				result += ((RpcClientConnectionMBean) connection).getRejectedRequests();
			}
		}
		return result;
	}

	@Override
	public long getTotalFailedRequests() {
		long result = 0;
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				result += ((RpcClientConnectionMBean) connection).getFailedRequests();
			}
		}
		return result;
	}

	@Override
	public long getTotalExpiredRequests() {
		long result = 0;
		for (RpcClientConnection connection : connections.values()) {
			if (connection instanceof RpcClientConnectionMBean) {
				result += ((RpcClientConnectionMBean) connection).getExpiredRequests();
			}
		}
		return result;
	}
}
