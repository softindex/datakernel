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

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.util.MemSize;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.time.Duration;
import java.util.ArrayDeque;

import static io.datakernel.eventloop.AsyncTcpSocketImpl.OP_POSTPONED;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class AsyncUdpSocketImpl implements AsyncUdpSocket, NioChannelEventHandler {
	private static final MemSize DEFAULT_UDP_BUFFER_SIZE = MemSize.kilobytes(16);

	private final Eventloop eventloop;

	@Nullable
	private SelectionKey key;

	private int receiveBufferSize = DEFAULT_UDP_BUFFER_SIZE.toInt();

	private final DatagramChannel channel;
	private final ArrayDeque<UdpPacket> writeQueue = new ArrayDeque<>();

	private AsyncUdpSocket.EventHandler eventHandler;

	private int ops = 0;

	// region JMX
	public interface Inspector {
		void onReceive(UdpPacket packet);

		void onReceiveError(IOException e);

		void onSend(UdpPacket packet);

		void onSendError(IOException e);
	}

	public static class JmxInspector implements Inspector {
		private final ValueStats receives;
		private final EventStats receiveErrors;
		private final ValueStats sends;
		private final EventStats sendErrors;

//		public JmxInspector(double smoothingWindow) {
//			this.receives = ValueStats.create(smoothingWindow);
//			this.receiveErrors = EventStats.create(smoothingWindow);
//			this.sends = ValueStats.create(smoothingWindow);
//			this.sendErrors = EventStats.create(smoothingWindow);
//		}

		public JmxInspector(Duration smoothingWindow) {
			this.receives = ValueStats.create(smoothingWindow).withUnit("bytes").withRate();
			this.receiveErrors = EventStats.create(smoothingWindow);
			this.sends = ValueStats.create(smoothingWindow).withUnit("bytes").withRate();
			this.sendErrors = EventStats.create(smoothingWindow);
		}

		@Override
		public void onReceive(UdpPacket packet) {
			receives.recordValue(packet.getBuf().readRemaining());
		}

		@Override
		public void onReceiveError(IOException e) {
			receiveErrors.recordEvent();
		}

		@Override
		public void onSend(UdpPacket packet) {
			sends.recordValue(packet.getBuf().readRemaining());
		}

		@Override
		public void onSendError(IOException e) {
			sendErrors.recordEvent();
		}

		@JmxAttribute(description = "Received packet size")
		public ValueStats getReceives() {
			return receives;
		}

		@JmxAttribute
		public EventStats getReceiveErrors() {
			return receiveErrors;
		}

		@JmxAttribute(description = "Sent packet size")
		public ValueStats getSends() {
			return sends;
		}

		@JmxAttribute
		public EventStats getSendErrors() {
			return sendErrors;
		}
	}

	@Nullable
	private Inspector inspector;
	// endregion

	// region creators
	private AsyncUdpSocketImpl(Eventloop eventloop, DatagramChannel channel) {
		this.eventloop = checkNotNull(eventloop);
		this.channel = checkNotNull(channel);
	}

	public static AsyncUdpSocketImpl create(Eventloop eventloop, DatagramChannel channel) {
		return new AsyncUdpSocketImpl(eventloop, channel);
	}

	public AsyncUdpSocketImpl withInspector(@Nullable Inspector inspector) {
		this.inspector = inspector;
		return this;
	}
	// endregion

	@Override
	public void setEventHandler(AsyncUdpSocket.EventHandler eventHandler) {
		this.eventHandler = eventHandler;
	}

	public void setReceiveBufferSize(int receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}

	public void register() {
		try {
			key = channel.register(eventloop.ensureSelector(), ops, this);
		} catch (IOException e) {
			eventloop.post(() -> {
				eventloop.closeChannel(channel);
				eventHandler.onClosedWithError(e);
			});
		}
		eventHandler.onRegistered();
	}

	public boolean isOpen() {
		return key != null;
	}

	@Override
	public void receive() {
		readInterest(true);
	}

	@Override
	public void onReadReady() {
		while (isOpen()) {
			ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
			ByteBuffer buffer = buf.toWriteByteBuffer();
			InetSocketAddress sourceAddress = null;
			try {
				sourceAddress = (InetSocketAddress) channel.receive(buffer);
			} catch (IOException e) {
				if (inspector != null) inspector.onReceiveError(e);
			}

			if (sourceAddress == null) {
				buf.recycle();
				break;
			}

			buf.ofWriteByteBuffer(buffer);
			UdpPacket packet = UdpPacket.of(buf, sourceAddress);
			if (inspector != null) inspector.onReceive(packet);
			eventHandler.onReceive(packet);
		}
	}

	@Override
	public void send(UdpPacket packet) {
		writeQueue.add(packet);
		onWriteReady();
	}

	@Override
	public void onWriteReady() {
		while (!writeQueue.isEmpty()) {
			UdpPacket packet = writeQueue.peek();
			assert packet != null; // isEmpty check
			ByteBuffer buffer = packet.getBuf().toReadByteBuffer();

			int needToSend = buffer.remaining();
			int sent = -1;

			try {
				sent = channel.send(buffer, packet.getSocketAddress());
			} catch (IOException e) {
				if (inspector != null) inspector.onSendError(e);
			}

			if (sent != needToSend) {
				break;
			}

			if (inspector != null) inspector.onSend(packet);

			writeQueue.poll();
			packet.recycle();
		}

		if (writeQueue.isEmpty()) {
			eventHandler.onSend();
			writeInterest(false);
		} else {
			writeInterest(true);
		}
	}

	@SuppressWarnings("MagicConstant")
	private void interests(int newOps) {
		if (ops != newOps) {
			ops = newOps;
			if ((ops & OP_POSTPONED) == 0 && key != null) {
				key.interestOps(ops);
			}
		}
	}

	@SuppressWarnings("SameParameterValue")
	private void readInterest(boolean readInterest) {
		interests(readInterest ? (ops | SelectionKey.OP_READ) : (ops & ~SelectionKey.OP_READ));
	}

	private void writeInterest(boolean writeInterest) {
		interests(writeInterest ? (ops | SelectionKey.OP_WRITE) : (ops & ~SelectionKey.OP_WRITE));
	}

	@Override
	public void close() {
		assert eventloop.inEventloopThread();
		if (key == null) {
			return;
		}
		eventloop.closeChannel(key);
		key = null;
		for (UdpPacket packet : writeQueue) {
			packet.recycle();
		}
		writeQueue.clear();
	}

	@Override
	public String toString() {
		if (isOpen()) {
			return getRemoteSocketAddress() + " " + eventHandler.toString();
		}
		return "<closed> " + eventHandler.toString();
	}

	private InetSocketAddress getRemoteSocketAddress() {
		try {
			return (InetSocketAddress) channel.getRemoteAddress();
		} catch (ClosedChannelException e) {
			throw new AssertionError("Channel is closed");
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
}
