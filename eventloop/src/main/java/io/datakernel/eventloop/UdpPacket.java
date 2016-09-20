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

import io.datakernel.bytebuf.ByteBuf;

import java.net.InetSocketAddress;

/**
 * This class represents a UDP packet. Each message is routed from one machine to another based solely on
 * information contained within that packet
 */
public final class UdpPacket {
	/**
	 * The data buffer to send
	 */
	private ByteBuf buf;
	/**
	 * The address to which the packet should be sent or from which it
	 * was received.
	 */
	private final InetSocketAddress inetSocketAddress;

	// region builders
	private UdpPacket(ByteBuf buf, InetSocketAddress inetSocketAddress) {
		this.buf = buf;
		this.inetSocketAddress = inetSocketAddress;
	}

	/**
	 * Creates a new instance of UDP packet
	 *
	 * @param buf               the data buffer to send or which was received
	 * @param inetSocketAddress the address to which the packet should be send or from which it
	 *                          was received
	 */
	public static UdpPacket of(ByteBuf buf, InetSocketAddress inetSocketAddress) {
		return new UdpPacket(buf, inetSocketAddress);
	}
	// endregion

	/**
	 * Returns the data buffer to send or which was received
	 */
	public ByteBuf getBuf() {
		return buf;
	}

	/**
	 * Returns the address to which the packet should be sent or from which it
	 * was received.
	 */
	public InetSocketAddress getSocketAddress() {
		return inetSocketAddress;
	}

	/**
	 * Recycles data buffer. You should do it after use.
	 */
	public void recycle() {
		if (buf != null) {
			buf.recycle();
			buf = null;
		}
	}
}
