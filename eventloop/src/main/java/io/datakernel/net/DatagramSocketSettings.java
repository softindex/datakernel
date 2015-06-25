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

import java.io.IOException;
import java.nio.channels.DatagramChannel;

import static com.google.common.base.Preconditions.checkState;
import static java.net.StandardSocketOptions.*;

/**
 * This class used to change settings for socket. It will be applying with creating new socket
 */
public class DatagramSocketSettings {
	private static final DatagramSocketSettings defaultSocketSettings = new DatagramSocketSettings();

	public static DatagramSocketSettings defaultDatagramSocketSettings() {
		return defaultSocketSettings;
	}

	protected static final int DEF_INT = -1;
	protected static final byte DEF_BOOL = -1;
	protected static final byte TRUE = 1;
	protected static final byte FALSE = 0;

	private final int receiveBufferSize;
	private final byte reuseAddress;
	private final int sendBufferSize;
	private final byte broadcast;

	protected DatagramSocketSettings(int receiveBufferSize, int sendBufferSize, byte reuseAddress,
	                                 byte broadcast) {
		this.receiveBufferSize = receiveBufferSize;
		this.reuseAddress = reuseAddress;
		this.sendBufferSize = sendBufferSize;
		this.broadcast = broadcast;
	}

	public DatagramSocketSettings() {
		this(DEF_INT, DEF_INT, DEF_BOOL, DEF_BOOL);
	}

	public DatagramSocketSettings receiveBufferSize(int receiveBufferSize) {
		return new DatagramSocketSettings(receiveBufferSize, sendBufferSize, reuseAddress, broadcast);
	}

	public DatagramSocketSettings sendBufferSize(int sendBufferSize) {
		return new DatagramSocketSettings(receiveBufferSize, sendBufferSize, reuseAddress, broadcast);
	}

	public DatagramSocketSettings reuseAddress(boolean reuseAddress) {
		return new DatagramSocketSettings(receiveBufferSize, sendBufferSize, reuseAddress ? TRUE : FALSE, broadcast);
	}

	public DatagramSocketSettings broadcast(boolean broadcast) {
		return new DatagramSocketSettings(receiveBufferSize, sendBufferSize, reuseAddress, broadcast ? TRUE : FALSE);
	}

	public void applySettings(DatagramChannel channel) throws IOException {
		if (receiveBufferSize != DEF_INT) {
			channel.setOption(SO_RCVBUF, receiveBufferSize);
		}
		if (sendBufferSize != DEF_INT) {
			channel.setOption(SO_SNDBUF, sendBufferSize);
		}
		if (reuseAddress != DEF_BOOL) {
			channel.setOption(SO_REUSEADDR, reuseAddress != FALSE);
		}
		if (broadcast != DEF_BOOL) {
			channel.setOption(SO_BROADCAST, broadcast != FALSE);
		}
	}

	public boolean hasReceiveBufferSize() {
		return receiveBufferSize != DEF_INT;
	}

	public int getReceiveBufferSize() {
		checkState(hasReceiveBufferSize());
		return receiveBufferSize;
	}

	public boolean hasReuseAddress() {
		return reuseAddress != DEF_BOOL;
	}

	public boolean getReuseAddress() {
		checkState(hasReuseAddress());
		return reuseAddress != FALSE;
	}

	public boolean hasSendBufferSize() {
		return sendBufferSize != DEF_INT;
	}

	public int getSendBufferSize() {
		checkState(hasSendBufferSize());
		return sendBufferSize;
	}

	public boolean hasBroadcast() {
		return broadcast != DEF_BOOL;
	}

	public boolean getBroadcast() {
		checkState(hasBroadcast());
		return broadcast != FALSE;
	}
}
