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

package io.datakernel.eventloop;

import io.datakernel.async.AsyncCancellableStatus;
import io.datakernel.async.AsyncGetter;
import io.datakernel.async.ResultCallback;
import io.datakernel.net.SocketSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * Class which handles connections to some eventloop.
 */
public final class SocketReconnector implements AsyncGetter<SocketChannel> {
	private static final Logger logger = LoggerFactory.getLogger(SocketReconnector.class);

	public static final int RECONNECT_ALWAYS = Integer.MAX_VALUE;

	private final NioEventloop eventloop;
	private final InetSocketAddress address;
	private final SocketSettings socketSettings;
	private final int reconnectAttempts;
	private final long reconnectTimeout;

	/**
	 * Creates a new instance of SocketReconnector
	 *
	 * @param eventloop         eventloop to which its instance will be related
	 * @param address           address to which socketChannels will be connected.
	 * @param socketSettings    sockets settings for creating new sockets
	 * @param reconnectAttempts number for attempts to connect
	 * @param reconnectTimeout  time after which it will begin connect
	 */
	public SocketReconnector(NioEventloop eventloop, InetSocketAddress address, SocketSettings socketSettings,
	                         int reconnectAttempts, long reconnectTimeout) {
		this.eventloop = checkNotNull(eventloop);
		this.address = checkNotNull(address);
		this.socketSettings = checkNotNull(socketSettings);
		this.reconnectAttempts = reconnectAttempts;
		this.reconnectTimeout = reconnectTimeout;
	}

	/**
	 * Creates a new instance of SocketReconnector
	 *
	 * @param eventloop      eventloop to which its instance will be related
	 * @param address        address to which socketChannels will be connected.
	 * @param socketSettings sockets settings for creating new sockets
	 */
	public SocketReconnector(NioEventloop eventloop, InetSocketAddress address, SocketSettings socketSettings) {
		this(eventloop, address, socketSettings, 0, 0);
	}

	@Override
	public void get(final ResultCallback<SocketChannel> callback) {
		reconnect(eventloop, address, socketSettings, reconnectAttempts, reconnectTimeout, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				callback.onResult(socketChannel);
			}

			@Override
			public void onException(Exception exception) {
				callback.onException(exception);
			}
		});
	}

	/**
	 * Creates a new connection with settings from this reconnector.
	 *
	 * @param connectCallback callback which will be called after connecting
	 */
	public void reconnect(ConnectCallback connectCallback) {
		reconnect(eventloop, address, socketSettings, reconnectAttempts, reconnectTimeout, connectCallback);
	}

	/**
	 * Creates a new connection.
	 *
	 * @param eventloop         eventloop in which connection will be created
	 * @param address           address for connecting
	 * @param socketSettings    setting for creating socket
	 * @param reconnectAttempts number for attempts to connect
	 * @param reconnectTimeout  time after which it will begin connect
	 * @param callback          callback which handles result
	 */
	public static void reconnect(final NioEventloop eventloop, final InetSocketAddress address,
	                             final SocketSettings socketSettings,
	                             final int reconnectAttempts, final long reconnectTimeout,
	                             final ConnectCallback callback) {
		if (callback instanceof AsyncCancellableStatus && ((AsyncCancellableStatus) callback).isCancelled())
			return;
		logger.info("Connecting {}", address);
		eventloop.connect(address, socketSettings, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				logger.trace("Connection succeeded {}", socketChannel);
				callback.onConnect(socketChannel);
			}

			@Override
			public void onException(Exception exception) {
				if (reconnectAttempts > 0) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, exception.toString());
					}
					eventloop.scheduleBackground(eventloop.currentTimeMillis() + reconnectTimeout, new Runnable() {
						@Override
						public void run() {
							reconnect(eventloop, address, socketSettings,
									reconnectAttempts == RECONNECT_ALWAYS ? RECONNECT_ALWAYS : (reconnectAttempts - 1), reconnectTimeout,
									callback);
						}
					});
				} else {
					if (logger.isErrorEnabled()) {
						logger.error("Could not reconnect to {}: {}", address, exception.toString());
					}
					callback.onException(exception);
				}
			}
		});
	}
}
