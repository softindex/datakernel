package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;

import static io.datakernel.eventloop.AsyncTcpSocketImpl.OP_POSTPONED;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class AsyncUdpSocketImpl implements AsyncUdpSocket, NioChannelEventHandler {
	private static final int DEFAULT_UDP_BUFFER_SIZE = 16 * 1024;

	private final Eventloop eventloop;
	private SelectionKey key;

	private int receiveBufferSize = DEFAULT_UDP_BUFFER_SIZE;

	private final DatagramChannel channel;
	private final ArrayDeque<UdpPacket> writeQueue = new ArrayDeque<>();

	private AsyncUdpSocket.EventHandler eventHandler;

	private int ops = 0;

	// region creators && builder methods
	private AsyncUdpSocketImpl(Eventloop eventloop, DatagramChannel channel) {
		this.eventloop = checkNotNull(eventloop);
		this.channel = checkNotNull(channel);
	}

	public static AsyncUdpSocketImpl create(Eventloop eventloop, DatagramChannel channel) {
		return new AsyncUdpSocketImpl(eventloop, channel);
	}
	// endregion

	@Override
	public void setEventHandler(AsyncUdpSocket.EventHandler eventHandler) {
		this.eventHandler = eventHandler;
	}

	public void setReceiveBufferSize(int receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}

	//  miscellaneous
	public void register() {
		try {
			key = channel.register(eventloop.ensureSelector(), ops, this);
		} catch (final IOException e) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					closeChannel();
					eventHandler.onClosedWithError(e);
				}
			});
		}
		eventHandler.onRegistered();
	}

	public final boolean isOpen() {
		return key != null;
	}

	//  read cycle
	@Override
	public void read() {
		readInterest(true);
	}

	@Override
	public void onReadReady() {
		while (isOpen()) {
			ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
			ByteBuffer buffer = buf.toTailByteBuffer();
			InetSocketAddress sourceAddress;
			try {
				sourceAddress = (InetSocketAddress) channel.receive(buffer);
			} catch (IOException e) {
				buf.recycle();
				closeWithError(e);
				return;
			}

			if (sourceAddress == null) {
				buf.recycle();
				break;
			}

			buf.ofTailByteBuffer(buffer);
			eventHandler.onRead(UdpPacket.of(buf, sourceAddress));
		}
	}

	//  write cycle
	@Override
	public void send(UdpPacket packet) {
		writeQueue.add(packet);
		onWriteReady();
	}

	@Override
	public void onWriteReady() {
		while (!writeQueue.isEmpty()) {
			UdpPacket packet = writeQueue.peek();
			ByteBuffer buffer = packet.getBuf().toHeadByteBuffer();

			int needToSend = buffer.remaining();
			int sent;

			try {
				sent = channel.send(buffer, packet.getSocketAddress());
			} catch (IOException e) {
				closeWithError(e);
				return;
			}

			if (sent != needToSend) {
				break;
			}

			writeQueue.poll();
			packet.recycle();
		}

		if (writeQueue.isEmpty()) {
			eventHandler.onSent();
			writeInterest(false);
		} else {
			writeInterest(true);
		}
	}

	// interests management
	@SuppressWarnings("MagicConstant")
	private void interests(int newOps) {
		if (ops != newOps) {
			ops = newOps;
			if ((ops & OP_POSTPONED) == 0 && key != null) {
				key.interestOps(ops);
			}
		}
	}

	private void readInterest(boolean readInterest) {
		interests(readInterest ? (ops | SelectionKey.OP_READ) : (ops & ~SelectionKey.OP_READ));
	}

	private void writeInterest(boolean writeInterest) {
		interests(writeInterest ? (ops | SelectionKey.OP_WRITE) : (ops & ~SelectionKey.OP_WRITE));
	}

	//  close handling
	@Override
	public void close() {
		assert eventloop.inEventloopThread();
		if (key == null) return;
		closeChannel();
		key = null;
		for (UdpPacket packet : writeQueue) {
			packet.recycle();
		}
		writeQueue.clear();
	}

	private void closeChannel() {
		if (channel == null) return;
		try {
			channel.close();
		} catch (IOException e) {
			eventloop.recordIoError(e, toString());
		}
	}

	private void closeWithError(final IOException e) {
		if (isOpen()) {
			eventloop.recordIoError(e, this);
			close();
			eventHandler.onClosedWithError(e);
		}
	}

	@Override
	public String toString() {
		return getRemoteSocketAddress() + " " + eventHandler.toString();
	}

	private InetSocketAddress getRemoteSocketAddress() {
		try {
			return (InetSocketAddress) channel.getRemoteAddress();
		} catch (IOException ignored) {
			throw new AssertionError("I/O error occurs or channel closed");
		}
	}
}
