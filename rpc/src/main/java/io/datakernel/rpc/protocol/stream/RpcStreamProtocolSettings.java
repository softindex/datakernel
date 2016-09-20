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

package io.datakernel.rpc.protocol.stream;

import io.datakernel.stream.processor.StreamBinarySerializer;

import static io.datakernel.util.Preconditions.checkArgument;

public final class RpcStreamProtocolSettings {
	public static final int DEFAULT_PACKET_SIZE = 16;
	public static final int MAX_PACKET_SIZE = StreamBinarySerializer.MAX_SIZE;

	private int defaultPacketSize = DEFAULT_PACKET_SIZE;
	private int maxPacketSize = MAX_PACKET_SIZE;
	private boolean compression;

	private RpcStreamProtocolSettings() {}

	public static RpcStreamProtocolSettings create() {return new RpcStreamProtocolSettings();}

	public RpcStreamProtocolSettings packetSize(int defaultPacketSize, int maxPacketSize) {
		checkArgument(defaultPacketSize > 0);
		checkArgument(maxPacketSize >= defaultPacketSize);
		this.defaultPacketSize = defaultPacketSize;
		this.maxPacketSize = maxPacketSize;
		return this;
	}

	public RpcStreamProtocolSettings compression(boolean compression) {
		this.compression = compression;
		return this;
	}

	public int getDefaultPacketSize() {
		return defaultPacketSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public boolean isCompression() {
		return compression;
	}

}
