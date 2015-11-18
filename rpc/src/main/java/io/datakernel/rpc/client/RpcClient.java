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

import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.eventloop.SocketReconnector;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.jmx.MBeanFormat;
import io.datakernel.jmx.MBeanUtils;
import io.datakernel.net.ConnectSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.RpcClientConnection.StatusListener;
import io.datakernel.rpc.client.sender.RpcNoSenderAvailableException;
import io.datakernel.rpc.client.sender.RpcRequestSender;
import io.datakernel.rpc.client.sender.RpcRequestSenderHolder;
import io.datakernel.rpc.client.sender.RpcRequestSendingStrategy;
import io.datakernel.rpc.protocol.RpcMessageSerializer;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.openmbean.CompositeData;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.async.AsyncCallbacks.waitAny;
import static io.datakernel.util.Preconditions.*;

public final class RpcClient implements NioService, RpcClientMBean {

	@SuppressWarnings("unused")
	public static class Builder {
		private final NioEventloop eventloop;
		private final RpcClientSettings settings;
		private RpcMessageSerializer serializer;
		private RpcProtocolFactory protocolFactory;
		private RpcRequestSendingStrategy requestSendingStrategy;
		private Object pingMessage;
		private Integer countAwaitsConnects;
		private Logger parentLogger;

		public Builder(NioEventloop eventloop) {
			this(eventloop, new RpcClientSettings());
		}

		public Builder(NioEventloop eventloop, RpcClientSettings settings) {
			this.eventloop = checkNotNull(eventloop);
			this.settings = checkNotNull(settings);
		}

		public Builder serializer(RpcMessageSerializer serializer) {
			this.serializer = serializer;
			return this;
		}

		public Builder protocolFactory(RpcProtocolFactory protocolFactory) {
			this.protocolFactory = protocolFactory;
			return this;
		}

		public Builder requestSendingStrategy(RpcRequestSendingStrategy requestSendingStrategy) {
			this.requestSendingStrategy = requestSendingStrategy;
			return this;
		}

		public Builder pingMessage(Object pingMessage) {
			checkArgument(settings.getPingAmountFailed() >= 0);
			this.pingMessage = checkNotNull(pingMessage);
			return this;
		}

		public Builder addresses(List<InetSocketAddress> addresses) {
			this.settings.addresses(Collections.unmodifiableList(new ArrayList<>(addresses)));
			return this;
		}

		public Builder socketSettings(SocketSettings socketSettings) {
			this.settings.socketSettings(socketSettings);
			return this;
		}

		public Builder connectSettings(ConnectSettings connectSettings) {
			this.settings.connectSettings(connectSettings);
			return this;
		}

		public Builder noWaitConnected() {
			return countAwaitsConnects(0);
		}

		public Builder waitForFirstConnected() {
			return countAwaitsConnects(1);
		}

		public Builder waitForAllConnected() {
			return countAwaitsConnects(RpcClientSettings.DEFAULT_ALL_CONNECTIONS);
		}

		public Builder countAwaitsConnects(int countAwaitsConnects) {
			checkState(this.countAwaitsConnects == null, "countAwaitsConnects already set");
			this.countAwaitsConnects = countAwaitsConnects;
			return this;
		}

		public Builder parentLogger(Logger logger) {
			checkNotNull(logger, "Logger must not be null");
			this.parentLogger = logger;
			return this;
		}

		public RpcClient build() {
			checkNotNull(serializer, "RpcMessageSerializer is no set");
			checkNotNull(protocolFactory, "RpcProtocolFactory is no set");
			checkNotNull(requestSendingStrategy, "RequestSenderFactory is not set");
			checkNotNull(settings.getAddresses(), "Addresses is not set");
			return new RpcClient(this);
		}

		private int getCountAwaitsConnects() {
			if (countAwaitsConnects == null || countAwaitsConnects == RpcClientSettings.DEFAULT_ALL_CONNECTIONS) {
				return Math.min(settings.getMinAliveConnections(), settings.getAddresses().size());
			}
			checkArgument(countAwaitsConnects >= 0 && countAwaitsConnects <= settings.getAddresses().size());
			return countAwaitsConnects;
		}
	}

	private final Logger logger;
	private final RpcClientConnectionPool connections;
	private final NioEventloop eventloop;
	private final List<InetSocketAddress> addresses;
	private final RpcProtocolFactory protocolFactory;
	private final RpcMessageSerializer serializer;
	private final RpcRequestSendingStrategy requestSendingStrategy;
	private final SocketSettings socketSettings;
	private final ConnectSettings connectSettings;
	private final int countAwaitsConnects;
	private final int timeoutPrecision;
	private final Map<InetSocketAddress, Long> pingTimestamps = new HashMap<>();
	private final Object pingMessage;
	private final long pingIntervalMillis;
	private final long pingAmountFailed;

	private RpcRequestSender requestSender;

	private AsyncCancellable schedulePingTask;
	private boolean running;

	// JMX
	private final String addressesString;
	private final LastExceptionCounter lastException = new LastExceptionCounter("LastException");
	private int successfulConnects = 0;
	private int failedConnects = 0;
	private int closedConnects = 0;
	private int pingReconnects = 0;

	private RpcClient(Builder builder) {
		this.eventloop = builder.eventloop;
		this.addresses = builder.settings.getAddresses();
		this.connections = new RpcClientConnectionPool(addresses);
		this.protocolFactory = builder.protocolFactory;
		this.serializer = builder.serializer;
		this.requestSendingStrategy = builder.requestSendingStrategy;
		RpcRequestSenderHolder holder = requestSendingStrategy.create(connections);
		this.requestSender = holder.isSenderPresent() ? holder.getSender() : new RequestSenderError();
		this.socketSettings = builder.settings.getSocketSettings();
		this.connectSettings = builder.settings.getConnectSettings();
		this.countAwaitsConnects = builder.getCountAwaitsConnects();
		this.timeoutPrecision = builder.settings.getTimeoutPrecision();
		this.pingMessage = builder.pingMessage;
		this.pingIntervalMillis = builder.settings.getPingIntervalMillis();
		this.pingAmountFailed = builder.settings.getPingAmountFailed();
		this.addressesString = addresses.toString();
		this.logger = LoggerFactory.getLogger((builder.parentLogger == null) ? RpcClient.class.getSimpleName() :
				builder.parentLogger.getName() + "$" + RpcClient.class.getSimpleName());
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		assert eventloop.inEventloopThread();
		checkNotNull(callback);

		if (running) {
			callback.onComplete();
			return;
		}
		running = true;
		CompletionCallback connectTimeoutWrapper = scheduleConnectTimeout(callback);
		CompletionCallback waitAny = waitAny(countAwaitsConnects, addresses.size(), connectTimeoutWrapper);
		for (InetSocketAddress address : addresses) {
			connect(address, connectSettings.attemptsReconnection(), waitAny);
		}
	}

	public void stop() {
		assert eventloop.inEventloopThread();
		if (!running) return;
		running = false;
		if (schedulePingTask != null)
			schedulePingTask.cancel();
		closeConnections();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		stop();
		callback.onComplete();
	}

	private CompletionCallback scheduleConnectTimeout(final CompletionCallback connectCompletion) {
		long connectTimeoutMillis = connectSettings.connectTimeoutMillis();
		if (connectCompletion == null || connectTimeoutMillis == 0)
			return connectCompletion;
		final CompletionCallback connectTimeoutWrapper = new CompletionCallback() {
			private boolean completed = false;

			@Override
			public void onComplete() {
				if (completed)
					return;
				completed = true;
				connectCompletion.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				if (completed)
					return;
				completed = true;
				logger.error(exception.getMessage());
				connectCompletion.onException(exception);
			}
		};
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + connectTimeoutMillis, new Runnable() {
			@Override
			public void run() {
				String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
						connectSettings.connectTimeoutMillis() / 1000.0);
				connectTimeoutWrapper.onException(new InterruptedException(errorMsg));
			}
		});
		return connectTimeoutWrapper;
	}

	private void connect(final InetSocketAddress address, final int reconnectAttempts, final CompletionCallback connectCallback) {
		if (!running) {
			connectCallback.onComplete();
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
						connect(address, connectSettings.attemptsReconnection(), ignoreCompletionCallback());
					}
				};
				RpcClientConnection connection = new RpcClientConnectionImpl(eventloop, socketChannel, timeoutPrecision, serializer, protocolFactory, statusListener);
				connection.getSocketConnection().register();
				successfulConnects++;
				logger.info("Connection to {} established", address);
				connectCallback.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				lastException.update(exception, "Connect fail to " + address.toString(), eventloop.currentTimeMillis());
				failedConnects++;
				if (running && reconnectAttempts > 0) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, exception.toString());
					}
					scheduleReconnect(address, reconnectAttempts, connectCallback);
				} else {
					if (logger.isErrorEnabled()) {
						logger.error("Could not reconnect to {}: {}", address, exception.toString());
					}
					connectCallback.onException(exception);
				}
			}
		});
	}

	public void scheduleReconnect(final InetSocketAddress address, final int reconnectAttempts, final CompletionCallback connectCallback) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + connectSettings.reconnectIntervalMillis(), new Runnable() {
			@Override
			public void run() {
				if (reconnectAttempts == SocketReconnector.RECONNECT_ALWAYS) {
					connect(address, SocketReconnector.RECONNECT_ALWAYS, connectCallback);
				} else {
					connect(address, reconnectAttempts - 1, connectCallback);
				}
			}
		});
	}

	private void addConnection(InetSocketAddress address, RpcClientConnection connection) {
		connections.add(address, connection);
		RpcRequestSenderHolder holder = requestSendingStrategy.create(connections);
		requestSender = holder.isSenderPresent() ? holder.getSender() : new RequestSenderError();
		if (isPingEnabled()) {
			pingTimestamps.put(address, eventloop.currentTimeMillis());
			schedulePingTask(address);
		}
	}

	private void removeConnection(InetSocketAddress address) {
		connections.remove(address);
		if (isPingEnabled()) {
			pingTimestamps.remove(address);
		}
		RpcRequestSenderHolder holder = requestSendingStrategy.create(connections);
		requestSender = holder.isSenderPresent() ? holder.getSender() : new RequestSenderError();
	}

	private void closeConnections() {
		for (RpcClientConnection connection : connections.values()) {
			connection.close();
		}
	}

	private void schedulePingTask(final InetSocketAddress address) {
		if (!running)
			return;
		schedulePingTask = eventloop.scheduleBackground(eventloop.currentTimeMillis() + pingIntervalMillis, new Runnable() {
			@Override
			public void run() {
				pingToAddress(address);
				schedulePingTask(address);
			}
		});
	}

	private void pingToAddress(final InetSocketAddress address) {
		final RpcClientConnection connection = connections.get(address);
		if (connection == null)
			return;
		Long pingTimestamp = pingTimestamps.get(address);
		if (pingTimestamp == null || eventloop.currentTimeMillis() - pingTimestamp < pingIntervalMillis)
			return;

		connection.callMethod(pingMessage, (int) pingIntervalMillis, new ResultCallback<Object>() {
			@Override
			public void onResult(Object result) {
				pingTimestamps.put(address, eventloop.currentTimeMillis());
			}

			@Override
			public void onException(Exception exception) {
				Long timestamp = pingTimestamps.get(address);
				if (timestamp != null) {
					if (eventloop.currentTimeMillis() - timestamp > pingIntervalMillis * pingAmountFailed) {
						pingTimestamps.remove(address);
						logger.warn("Server {} does not respond for more then {} seconds for {} times. Reconnecting...",
								address, pingIntervalMillis / 1000.0, pingAmountFailed);
						pingReconnects++;
						connection.close();
					}
				}
			}
		});
	}

	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		requestSender.sendRequest(request, timeout, callback);
	}

	// visible for testing
	public RpcRequestSender getRequestSender() {
		return requestSender;
	}

	private static final class RequestSenderError implements RpcRequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No senders available");

		@Override
		public <T> void sendRequest(Object request,
		                            int timeout, ResultCallback<T> callback) {
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
	public void resetStats() {
		successfulConnects = 0;
		failedConnects = 0;
		closedConnects = 0;
		lastException.reset();
		pingReconnects = 0;
	}

	@Override
	public String getAddresses() {
		return addressesString;
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
	public boolean isPingEnabled() {
		return pingMessage != null;
	}

	@Override
	public int getPingReconnects() {
		return pingReconnects;
	}
}
