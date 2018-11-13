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

import io.datakernel.annotation.Nullable;
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
	private static final byte DEF_BOOL = -1;
	private static final byte TRUE = 1;
	private static final byte FALSE = 0;

	private final byte keepAlive;
	private final byte reuseAddress;
	private final byte tcpNoDelay;

	@Nullable
	private final MemSize sendBufferSize;
	@Nullable
	private final MemSize receiveBufferSize;
	@Nullable
	private final Duration implReadTimeout;
	@Nullable
	private final Duration implWriteTimeout;
	@Nullable
	private final MemSize implReadSize;
	@Nullable
	private final MemSize implWriteSize;

	// region builders
	private SocketSettings(@Nullable MemSize sendBufferSize, @Nullable MemSize receiveBufferSize, byte keepAlive, byte reuseAddress, byte tcpNoDelay, @Nullable Duration implReadTimeout, @Nullable Duration implWriteTimeout, @Nullable MemSize implReadSize, @Nullable MemSize implWriteSize) {
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
		return new SocketSettings(null, null, DEF_BOOL, DEF_BOOL, DEF_BOOL, null, null, null, null);
	}

	public SocketSettings withSendBufferSize(MemSize sendBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withReceiveBufferSize(MemSize receiveBufferSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
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
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplWriteTimeout(Duration implWriteTimeout) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplReadSize(MemSize implReadSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}

	public SocketSettings withImplWriteSize(MemSize implWriteSize) {
		return new SocketSettings(sendBufferSize, receiveBufferSize, keepAlive, reuseAddress, tcpNoDelay, implReadTimeout, implWriteTimeout, implReadSize, implWriteSize);
	}
	// endregion

	public void applySettings(SocketChannel channel) throws IOException {
		if (sendBufferSize != null) {
			channel.setOption(SO_SNDBUF, sendBufferSize.toInt());
		}
		if (receiveBufferSize != null) {
			channel.setOption(SO_RCVBUF, receiveBufferSize.toInt());
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
		return sendBufferSize != null;
	}

	public MemSize getSendBufferSize() {
		check(hasSendBufferSize());
		return sendBufferSize;
	}

	public boolean hasReceiveBufferSize() {
		return receiveBufferSize != null;
	}

	public MemSize getReceiveBufferSize() {
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
		return implReadTimeout != null;
	}

	public Duration getImplReadTimeout() {
		assert hasImplReadTimeout();
		return implReadTimeout;
	}

	public boolean hasImplWriteTimeout() {
		return implWriteTimeout != null;
	}

	public Duration getImplWriteTimeout() {
		assert hasImplWriteTimeout();
		return implWriteTimeout;
	}

	public boolean hasImplReadSize() {
		return implReadSize != null;
	}

	public MemSize getImplReadSize() {
		assert hasImplReadSize();
		return implReadSize;
	}

	public boolean hasImplWriteSize() {
		return implWriteSize != null;
	}

	public MemSize getImplWriteSize() {
		assert hasImplWriteSize();
		return implWriteSize;
	}

}
