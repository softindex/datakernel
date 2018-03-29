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

import io.datakernel.util.MemSize;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.time.Duration;

import static io.datakernel.util.Preconditions.check;
import static java.net.StandardSocketOptions.*;

/**
 * This class used to change settings for socket. It will be applying with creating new socket
 */
public final class SocketSettings {
	protected static final int DEF_INT = -1;
	protected static final byte DEF_BOOL = -1;
	protected static final byte TRUE = 1;
	protected static final byte FALSE = 0;

	private final int sendBufferSize;
	private final int receiveBufferSize;
	private final byte keepAlive;
	private final byte reuseAddress;
	private final byte tcpNoDelay;

	private final long implReadTimeout;
	private final long implWriteTimeout;
	private final int implReadSize;
	private final int implWriteSize;

	// region builders
	private SocketSettings(int sendBufferSize, int receiveBufferSize, byte keepAlive, byte reuseAddress, byte tcpNoDelay, long implReadTimeout, long implWriteTimeout, int implReadSize, int implWriteSize) {
		this.sendBufferSize = sendBufferSize;
		this.receiveBufferSize = receiveBufferSize;
		this.keepAlive = keepAlive;
		this.reuseAddress = reuseAddress;
		this.tcpNoDelay = tcpNoDelay;
		this.implReadTimeout = implReadTimeout;
		this.implWriteTimeout = implWriteTimeout;
		this.implReadSize = implReadSize;
		this.implWriteSize = implWriteSize;
	}

	public static SocketSettings create() {
		return new SocketSettings(DEF_INT, DEF_INT, DEF_BOOL, DEF_BOOL, DEF_BOOL, DEF_INT, DEF_INT, DEF_INT, DEF_INT);
	}

	public SocketSettings withSendBufferSize(int sendBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withSendBufferSize(MemSize sendBufferSize) {
		return withSendBufferSize(sendBufferSize.toInt());
	}

	public SocketSettings withReceiveBufferSize(int receiveBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withReceiveBufferSize(MemSize receiveBufferSize) {
		return withReceiveBufferSize(receiveBufferSize.toInt());
	}

	public SocketSettings withKeepAlive(boolean keepAlive) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive ? TRUE : FALSE, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withReuseAddress(boolean reuseAddress) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress ? TRUE : FALSE, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withTcpNoDelay(boolean tcpNoDelay) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay ? TRUE : FALSE, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplReadTimeout(Duration implReadTimeout) {
		return withImplReadTimeout(implReadTimeout.toMillis());
	}

	public SocketSettings withImplReadTimeout(long implReadTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplWriteTimeout(Duration implWriteTimeout) {
		return withImplWriteTimeout(implWriteTimeout.toMillis());
	}

	public SocketSettings withImplWriteTimeout(long implWriteTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplReadSize(int implReadSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplReadSize(MemSize implReadSize) {
		return withImplReadSize(implReadSize.toInt());
	}

	public SocketSettings withImplWriteSize(int implWriteSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplWriteSize(MemSize implWriteSize) {
		return withImplWriteSize(implWriteSize.toInt());
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

	public boolean hasImplReadTimeout() {
		return implReadTimeout != DEF_INT;
	}

	public long getImplReadTimeout() {
		assert hasImplReadTimeout();
		return implReadTimeout;
	}

	public boolean hasImplWriteTimeout() {
		return implWriteTimeout != DEF_INT;
	}

	public long getImplWriteTimeout() {
		assert hasImplWriteTimeout();
		return implWriteTimeout;
	}

	public boolean hasImplReadSize() {
		return implReadSize != DEF_INT;
	}

	public int getImplReadSize() {
		assert hasImplReadSize();
		return implReadSize;
	}

	public boolean hasImplWriteSize() {
		return implWriteSize != DEF_INT;
	}

	public int getImplWriteSize() {
		assert hasImplWriteSize();
		return implWriteSize;
	}

}
