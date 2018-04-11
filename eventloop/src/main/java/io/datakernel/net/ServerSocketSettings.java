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
import java.nio.channels.ServerSocketChannel;

import static io.datakernel.util.Preconditions.check;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;

/**
 * This class used to change settings for server socket. It will be applying with creating new server socket
 */
public final class ServerSocketSettings {
	public static final MemSize DEFAULT_BACKLOG = MemSize.kilobytes(16);

	protected static final byte DEF_BOOL = -1;
	protected static final byte TRUE = 1;
	protected static final byte FALSE = 0;

	@Nullable
	private final MemSize backlog;
	@Nullable
	private final MemSize receiveBufferSize;
	private final byte reuseAddress;

	// region builders
	private ServerSocketSettings(MemSize backlog, MemSize receiveBufferSize, byte reuseAddress) {
		this.backlog = backlog;
		this.receiveBufferSize = receiveBufferSize;
		this.reuseAddress = reuseAddress;
	}

	public static ServerSocketSettings create(MemSize backlog) {return new ServerSocketSettings(backlog, null, DEF_BOOL);}

	public ServerSocketSettings withBacklog(MemSize backlog) {
		return new ServerSocketSettings(backlog, receiveBufferSize, reuseAddress);
	}

	public ServerSocketSettings withReceiveBufferSize(MemSize receiveBufferSize) {
		return new ServerSocketSettings(backlog, receiveBufferSize, reuseAddress);
	}

	public ServerSocketSettings withReuseAddress(boolean reuseAddress) {
		return new ServerSocketSettings(backlog, receiveBufferSize, reuseAddress ? TRUE : FALSE);
	}
	// endregion

	public void applySettings(ServerSocketChannel channel) throws IOException {
		if (receiveBufferSize != null) {
			channel.setOption(SO_RCVBUF, receiveBufferSize.toInt());
		}
		if (reuseAddress != DEF_BOOL) {
			channel.setOption(SO_REUSEADDR, reuseAddress != FALSE);
		}
	}

	public MemSize getBacklog() {
		return backlog;
	}

	public boolean hasReceiveBufferSize() {
		return receiveBufferSize != null;
	}

	public MemSize getReceiveBufferSize() {
		check(hasReceiveBufferSize());
		return receiveBufferSize;
	}

	public boolean hasReuseAddress() {
		return reuseAddress != DEF_BOOL;
	}

	public boolean getReuseAddress() {
		check(hasReuseAddress());
		return reuseAddress != FALSE;
	}
}
