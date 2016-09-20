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

package io.datakernel.net;

import io.datakernel.eventloop.AsyncTcpSocketImpl;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static io.datakernel.util.Preconditions.check;
import static java.net.StandardSocketOptions.*;

/**
 * This class used to change settings for socket. It will be applying with creating new socket
 */
public final class SocketSettings {
	private static final SocketSettings DEFAULT_SOCKET_SETTINGS = create();

	public static SocketSettings defaultSocketSettings() {
		return DEFAULT_SOCKET_SETTINGS;
	}

	protected static final int DEF_INT = -1;
	protected static final byte DEF_BOOL = -1;
	protected static final byte TRUE = 1;
	protected static final byte FALSE = 0;

	private final int sendBufferSize;
	private final int receiveBufferSize;
	private final byte keepAlive;
	private final byte reuseAddress;
	private final byte tcpNoDelay;
	private final long readTimeout;
	private final long writeTimeout;

	// region builders
	private SocketSettings(int sendBufferSize, int receiveBufferSize, byte keepAlive, byte reuseAddress, byte tcpNoDelay, long readTimeout, long writeTimeout) {
		this.sendBufferSize = sendBufferSize;
		this.receiveBufferSize = receiveBufferSize;
		this.keepAlive = keepAlive;
		this.reuseAddress = reuseAddress;
		this.tcpNoDelay = tcpNoDelay;
		this.readTimeout = readTimeout;
		this.writeTimeout = writeTimeout;
	}

	public static SocketSettings create() {
		return new SocketSettings(DEF_INT, DEF_INT, DEF_BOOL, DEF_BOOL, DEF_BOOL, DEF_INT, DEF_INT);
	}

	public SocketSettings withSendBufferSize(int sendBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, readTimeout, writeTimeout);
	}

	public SocketSettings withReceiveBufferSize(int receiveBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, readTimeout, writeTimeout);
	}

	public SocketSettings withKeepAlive(boolean keepAlive) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive ? TRUE : FALSE, reuseAddress, tcpNoDelay, readTimeout, writeTimeout);
	}

	public SocketSettings withReuseAddress(boolean reuseAddress) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress ? TRUE : FALSE, tcpNoDelay, readTimeout, writeTimeout);
	}

	public SocketSettings withTcpNoDelay(boolean tcpNoDelay) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay ? TRUE : FALSE, readTimeout, writeTimeout);
	}

	public SocketSettings withReadTimeout(long readTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, readTimeout, writeTimeout);
	}

	public SocketSettings withWriteTimeout(long writeTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, readTimeout, writeTimeout);
	}
	// endregion

	public void applySettings(SocketChannel channel) throws IOException {
		if (sendBufferSize != DEF_INT) {
			channel.setOption(SO_SNDBUF, sendBufferSize);
		}
		if (receiveBufferSize != DEF_INT) {
			channel.setOption(SO_RCVBUF, receiveBufferSize);
		}
		if (keepAlive != DEF_BOOL) {
			channel.setOption(SO_KEEPALIVE, keepAlive != FALSE);
		}
		if (reuseAddress != DEF_BOOL) {
			channel.setOption(SO_REUSEADDR, reuseAddress != FALSE);
		}
		if (tcpNoDelay != DEF_BOOL) {
			channel.setOption(TCP_NODELAY, tcpNoDelay != FALSE);
		}
	}

	public void applyReadWriteTimeoutsTo(AsyncTcpSocketImpl asyncTcpSocket) {
		if (hasReadTimeout()) {
			asyncTcpSocket.readTimeout(readTimeout);
		}
		if (hasWriteTimeout()) {
			asyncTcpSocket.writeTimeout(writeTimeout);
		}
	}

	public boolean hasSendBufferSize() {
		return sendBufferSize != DEF_INT;
	}

	public int getSendBufferSize() {
		check(hasSendBufferSize());
		return sendBufferSize;
	}

	public boolean hasReceiveBufferSize() {
		return receiveBufferSize != DEF_INT;
	}

	public int getReceiveBufferSize() {
		check(hasReceiveBufferSize());
		return receiveBufferSize;
	}

	public boolean hasKeepAlive() {
		return keepAlive != DEF_BOOL;
	}

	public boolean getKeepAlive() {
		check(hasKeepAlive());
		return keepAlive != FALSE;
	}

	public boolean hasReuseAddress() {
		return reuseAddress != DEF_BOOL;
	}

	public boolean getReuseAddress() {
		check(hasReuseAddress());
		return reuseAddress != FALSE;
	}

	public boolean hasTcpNoDelay() {
		return tcpNoDelay != DEF_BOOL;
	}

	public boolean getTcpNoDelay() {
		check(hasTcpNoDelay());
		return tcpNoDelay != FALSE;
	}

	public boolean hasReadTimeout() {
		return readTimeout != DEF_INT;
	}

	public long getReadTimeout() {
		assert hasReadTimeout();
		return readTimeout;
	}

	public boolean hasWriteTimeout() {
		return writeTimeout != DEF_INT;
	}

	public long getWriteTimeout() {
		assert hasWriteTimeout();
		return writeTimeout;
	}
}
