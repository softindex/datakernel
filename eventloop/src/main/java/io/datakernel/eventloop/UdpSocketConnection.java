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
import io.datakernel.bytebuf.ByteBufPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.util.ArrayDeque;

/**
 * Represent I/O handler of UDP {@link DatagramChannel}
 */
public abstract class UdpSocketConnection extends SocketConnection {
	public static final int DEFAULT_UDP_BUFFER_SIZE = 16384;

	protected final DatagramChannel channel;
	protected final ArrayDeque<UdpPacket> writeQueue = new ArrayDeque<>();

	/**
	 * Creates new instance of UDP connection
	 *
	 * @param eventloop       eventloop in which will handle this connection
	 * @param datagramChannel channel for creating this connection
	 */
	public UdpSocketConnection(NioEventloop eventloop, DatagramChannel datagramChannel) {
		super(eventloop);
		this.channel = datagramChannel;
		this.receiveBufferSize = DEFAULT_UDP_BUFFER_SIZE;
	}

	/**
	 * This method reads received bytes, creates UDP packet with it and call its method onRead() with this packet.
	 */
	@Override
	public void onReadReady() {
		ByteBuf buf = null;
		try {
			while (true) {
				buf = ByteBufPool.allocate(receiveBufferSize);
				ByteBuffer byteBuffer = buf.toByteBuffer();
				SocketAddress sourceAddress = channel.receive(byteBuffer);
				buf.setByteBuffer(byteBuffer);

				if (sourceAddress == null) {
					break;
				}

				buf.flip();
				UdpPacket packet = new UdpPacket(buf, (InetSocketAddress) sourceAddress);
				onRead(packet);
				buf = null;
			}
		} catch (IOException e) {
			onInternalException(e);
		} finally {
			if (buf != null)
				buf.recycle();
		}
	}

	/**
	 * It processes received UDP packet to this server.
	 *
	 * @param packet the received packet.
	 */
	protected abstract void onRead(UdpPacket packet);

	/**
	 * Sends UDPpacket  from argument
	 *
	 * @param packet packet for sending
	 */
	public void send(UdpPacket packet) {
		writeQueue.add(packet);
		onWriteReady();
	}

	/**
	 * This method takes UDP packet from writeQueue, and send its bytes to address.
	 */
	@Override
	public void onWriteReady() {
		boolean wasWritten = false;

		while (!writeQueue.isEmpty()) {
			UdpPacket packet = writeQueue.peek();
			ByteBuffer buf = packet.getBuf().toByteBuffer();

			int remainingBeforeWrite = buf.remaining();
			int sent;

			try {
				sent = channel.send(buf, packet.getSocketAddress());
			} catch (IOException e) {
				onInternalException(e);
				return;
			}

			if (sent == remainingBeforeWrite) {
				wasWritten = true;
			} else {
				break;
			}

			writeQueue.poll();
			packet.recycle();
		}

		if (wasWritten) {
			writeTime = eventloop.currentTimeMillis();
		}

		if (writeQueue.isEmpty()) {
			try {
				onWriteFlushed();
			} catch (Exception e) {
				onInternalException(e);
			}
			writeInterest(false);
		} else {
			writeInterest(true);
		}
	}

	@Override
	public final SelectableChannel getChannel() {
		return this.channel;
	}

	/**
	 * After closing this connection it clears readQueue and writeQueue.
	 */
	@Override
	public void onClosed() {
		for (UdpPacket packet : writeQueue) {
			packet.recycle();
		}
		writeQueue.clear();
	}

	public String getChannelInfo() {
		return channel.toString();
	}

}
