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
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.bytebuf.ByteBufPool.pack;
import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings({"WeakerAccess", "AssertWithSideEffects"})
public final class AsyncTcpSocketImpl implements AsyncTcpSocket, NioChannelEventHandler {
	public static final MemSize DEFAULT_READ_BUF_SIZE = MemSize.kilobytes(16);
	public static final int OP_POSTPONED = 1 << 7;  // SelectionKey constant

	@SuppressWarnings("ThrowableInstanceNeverThrown")
	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException("timed out");
	public static final int NO_TIMEOUT = -1;

	private static final MemSize MAX_MERGE_SIZE = MemSize.kilobytes(16);
	private static final AtomicInteger connectionCount = new AtomicInteger(0);

	private final Eventloop eventloop;
	private final SocketChannel channel;
	private final ArrayDeque<ByteBuf> writeQueue = new ArrayDeque<>();
	private boolean writeEndOfStream;
	private EventHandler socketEventHandler;

	@Nullable
	private SelectionKey key;

	private int ops = 0;
	private long readTimestamp = 0L;
	private long writeTimestamp = 0L;

	private long readTimeout = NO_TIMEOUT;
	private long writeTimeout = NO_TIMEOUT;
	protected MemSize readMaxSize = DEFAULT_READ_BUF_SIZE;
	protected MemSize writeMaxSize = MAX_MERGE_SIZE;

	@Nullable
	private ScheduledRunnable checkReadTimeout;

	@Nullable
	private ScheduledRunnable checkWriteTimeout;

	public interface Inspector {
		void onReadTimeout();

		void onRead(ByteBuf buf);

		void onReadEndOfStream();

		void onReadError(IOException e);

		void onWriteTimeout();

		void onWrite(ByteBuf buf, int bytes);

		void onWriteError(IOException e);
	}

	public static class JmxInspector implements Inspector {
		public static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final ValueStats reads = ValueStats.create(SMOOTHING_WINDOW).withUnit("bytes").withRate();
		private final EventStats readEndOfStreams = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats readErrors = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats readTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final ValueStats writes = ValueStats.create(SMOOTHING_WINDOW).withUnit("bytes").withRate();
		private final EventStats writeErrors = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats writeTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats writeOverloaded = EventStats.create(SMOOTHING_WINDOW);

		@Override
		public void onReadTimeout() {
			readTimeouts.recordEvent();
		}

		@Override
		public void onRead(ByteBuf buf) {
			reads.recordValue(buf.readRemaining());
		}

		@Override
		public void onReadEndOfStream() {
			readEndOfStreams.recordEvent();
		}

		@Override
		public void onReadError(IOException e) {
			readErrors.recordEvent();
		}

		@Override
		public void onWriteTimeout() {
			writeTimeouts.recordEvent();
		}

		@Override
		public void onWrite(ByteBuf buf, int bytes) {
			writes.recordValue(bytes);
			if (buf.readRemaining() != bytes)
				writeOverloaded.recordEvent();
		}

		@Override
		public void onWriteError(IOException e) {
			writeErrors.recordEvent();
		}

		@JmxAttribute
		public EventStats getReadTimeouts() {
			return readTimeouts;
		}

		@JmxAttribute
		public ValueStats getReads() {
			return reads;
		}

		@JmxAttribute
		public EventStats getReadEndOfStreams() {
			return readEndOfStreams;
		}

		@JmxAttribute
		public EventStats getReadErrors() {
			return readErrors;
		}

		@JmxAttribute
		public EventStats getWriteTimeouts() {
			return writeTimeouts;
		}

		@JmxAttribute
		public ValueStats getWrites() {
			return writes;
		}

		@JmxAttribute
		public EventStats getWriteErrors() {
			return writeErrors;
		}

		@JmxAttribute
		public EventStats getWriteOverloaded() {
			return writeOverloaded;
		}
	}

	@Nullable
	private Inspector inspector;

	// region builders
	public static AsyncTcpSocketImpl wrapChannel(Eventloop eventloop, SocketChannel socketChannel,
			SocketSettings socketSettings) {
		try {
			socketSettings.applySettings(socketChannel);
		} catch (IOException e) {
			throw new AssertionError("Failed to apply socketSettings", e);
		}
		AsyncTcpSocketImpl asyncTcpSocket = new AsyncTcpSocketImpl(eventloop, socketChannel);
		if (socketSettings.hasImplReadTimeout())
			asyncTcpSocket.readTimeout = socketSettings.getImplReadTimeout().toMillis();
		if (socketSettings.hasImplWriteTimeout())
			asyncTcpSocket.writeTimeout = socketSettings.getImplWriteTimeout().toMillis();
		if (socketSettings.hasImplReadSize())
			asyncTcpSocket.readMaxSize = socketSettings.getImplReadSize();
		if (socketSettings.hasImplWriteSize())
			asyncTcpSocket.writeMaxSize = socketSettings.getImplWriteSize();
		return asyncTcpSocket;
	}

	public static AsyncTcpSocketImpl wrapChannel(Eventloop eventloop, SocketChannel socketChannel) {
		return new AsyncTcpSocketImpl(eventloop, socketChannel);
	}

	public AsyncTcpSocketImpl withInspector(@Nullable Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	private AsyncTcpSocketImpl(Eventloop eventloop, SocketChannel socketChannel) {
		this.eventloop = checkNotNull(eventloop);
		this.channel = checkNotNull(socketChannel);
	}
	// endregion

	@Override
	public void setEventHandler(EventHandler eventHandler) {
		this.socketEventHandler = eventHandler;
	}

	public static int getConnectionCount() {
		return connectionCount.get();
	}

	public final void register() {
		socketEventHandler.onRegistered();
		try {
			key = channel.register(eventloop.ensureSelector(), ops, this);
			connectionCount.incrementAndGet();
		} catch (IOException e) {
			eventloop.post(() -> {
				eventloop.closeChannel(channel);
				socketEventHandler.onClosedWithError(e);
			});
		}
		if ((this.ops & SelectionKey.OP_READ) != 0) {
			onReadReady();
		}
	}

	// timeouts management
	void scheduleReadTimeOut() {
		if (checkReadTimeout == null) {
			checkReadTimeout = eventloop.delayBackground(readTimeout, () -> {
				if (inspector != null) inspector.onReadTimeout();
				checkReadTimeout = null;
				closeWithError(TIMEOUT_EXCEPTION, false);
			});
		}
	}

	void scheduleWriteTimeOut() {
		if (checkWriteTimeout == null) {
			checkWriteTimeout = eventloop.delayBackground(writeTimeout, () -> {
				if (inspector != null) inspector.onWriteTimeout();
				checkWriteTimeout = null;
				closeWithError(TIMEOUT_EXCEPTION, false);
			});
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

	@Override
	public void read() {
		if (readTimeout != NO_TIMEOUT) {
			scheduleReadTimeOut();
		}
		readInterest(true);
		if (readTimestamp == 0L) {
			readTimestamp = eventloop.currentTimeMillis();
			assert readTimestamp != 0L;
		}
	}

	@Override
	public void onReadReady() {
		readTimestamp = 0L;
		int oldOps = ops;
		ops = ops | OP_POSTPONED;
		readInterest(false);

		int bytesRead = doRead();
		if (bytesRead != 0) {
			int newOps = ops & ~OP_POSTPONED;
			ops = oldOps;
			interests(newOps);
		} else {
			ops = oldOps;
		}
	}

	private int doRead() {
		ByteBuf buf = ByteBufPool.allocate(readMaxSize);
		ByteBuffer buffer = buf.toWriteByteBuffer();

		int numRead;
		try {
			numRead = channel.read(buffer);
			buf.ofWriteByteBuffer(buffer);
		} catch (IOException e) {
			buf.recycle();
			if (inspector != null) inspector.onReadError(e);
			closeWithError(e, false);
			return -1;
		}

		if (numRead == 0) {
			if (inspector != null) inspector.onRead(buf);
			buf.recycle();
			return numRead;
		}

		if (checkReadTimeout != null) {
			checkReadTimeout.cancel();
			checkReadTimeout = null;
		}

		if (numRead == -1) {
			buf.recycle();
			if (inspector != null) inspector.onReadEndOfStream();
			socketEventHandler.onReadEndOfStream();
			return numRead;
		}

		if (inspector != null) inspector.onRead(buf);
		socketEventHandler.onRead(pack(buf));
		return numRead;
	}

	// write cycle
	@Override
	public void write(ByteBuf buf) {
		assert eventloop.inEventloopThread();
		if (writeTimeout != NO_TIMEOUT) {
			scheduleWriteTimeOut();
		}
		writeQueue.add(buf);
		postWriteRunnable();
	}

	@Override
	public void writeEndOfStream() {
		assert eventloop.inEventloopThread();
		if (writeEndOfStream) return;
		writeEndOfStream = true;
		postWriteRunnable();
	}

	@Override
	public void onWriteReady() {
		writeTimestamp = 0L;
		try {
			doWrite();
		} catch (IOException e) {
			closeWithError(e, false);
		}
	}

	private void doWrite() throws IOException {
		int writeMaxSize = this.writeMaxSize.toInt();

		while (true) {
			ByteBuf bufToSend = writeQueue.poll();
			if (bufToSend == null)
				break;

			while (true) {
				ByteBuf nextBuf = writeQueue.peek();
				if (nextBuf == null)
					break;

				int bytesToCopy = nextBuf.readRemaining(); // bytes to append to bufToSend
				if (bufToSend.readPosition() + bufToSend.readRemaining() + bytesToCopy > bufToSend.limit())
					bytesToCopy += bufToSend.readRemaining(); // append will resize bufToSend
				if (bytesToCopy < writeMaxSize) {
					bufToSend = ByteBufPool.append(bufToSend, nextBuf);
					writeQueue.poll();
				} else {
					break;
				}
			}

			@SuppressWarnings("ConstantConditions")
			ByteBuffer bufferToSend = bufToSend.toReadByteBuffer();

			try {
				channel.write(bufferToSend);
			} catch (IOException e) {
				if (inspector != null) inspector.onWriteError(e);
				bufToSend.recycle();
				throw e;
			}

			if (inspector != null)
				inspector.onWrite(bufToSend, bufferToSend.position() - bufToSend.readPosition());

			bufToSend.ofReadByteBuffer(bufferToSend);

			if (bufToSend.canRead()) {
				writeQueue.addFirst(bufToSend); // put the buf back to the queue, to send it the next time
				break;
			}
			bufToSend.recycle();
		}

		if (writeQueue.isEmpty()) {
			if (checkWriteTimeout != null) {
				checkWriteTimeout.cancel();
				checkWriteTimeout = null;
			}
			if (writeEndOfStream) {
				channel.shutdownOutput();
			}
			writeInterest(false);
			socketEventHandler.onWrite();
		} else {
			writeInterest(true);
		}
	}

	// close methods
	@Override
	public void close() {
		assert eventloop.inEventloopThread();
		if (key == null) return;
		eventloop.closeChannel(key);
		key = null;
		connectionCount.decrementAndGet();
		for (ByteBuf buf : writeQueue) {
			buf.recycle();
		}
		writeQueue.clear();
		if (checkWriteTimeout != null) {
			checkWriteTimeout.cancel();
			checkWriteTimeout = null;
		}
		if (checkReadTimeout != null) {
			checkReadTimeout.cancel();
			checkReadTimeout = null;
		}
	}

	private void closeWithError(Exception e, boolean fireAsync) {
		if (isOpen()) {
			close();
			if (fireAsync)
				eventloop.post(() -> socketEventHandler.onClosedWithError(e));
			else {
				socketEventHandler.onClosedWithError(e);
			}
		}
	}

	// miscellaneous
	private void postWriteRunnable() {
		if (writeTimestamp == 0L) {
			writeTimestamp = eventloop.currentTimeMillis();
			assert writeTimestamp != 0L;
			eventloop.post(() -> {
				if (writeTimestamp == 0L || !isOpen())
					return;
				writeTimestamp = 0L;
				try {
					doWrite();
				} catch (IOException e) {
					closeWithError(e, true);
				}
			});
		}
	}

	public boolean isOpen() {
		return key != null;
	}

	@Override
	public InetSocketAddress getRemoteSocketAddress() {
		try {
			return (InetSocketAddress) channel.getRemoteAddress();
		} catch (IOException ignored) {
			throw new AssertionError("I/O error occurs or channel closed");
		}
	}

	public SocketChannel getSocketChannel() {
		return channel;
	}

	@Override
	public String toString() {
		String keyOps;
		try {
			keyOps = (key == null ? "" : opsToString(key.interestOps()));
		} catch (Exception e) {
			keyOps = "Key throwed exception: " + e.toString();
		}
		return "AsyncTcpSocketImpl{" +
				"channel=" + (channel == null ? "" : channel.toString()) +
				", writeQueueSize=" + writeQueue.size() +
				", writeEndOfStream=" + writeEndOfStream +
				", key.ops=" + keyOps +
				", ops=" + opsToString(ops) +
				", writing=" + (writeTimestamp != 0L) +
				'}';
	}

	private String opsToString(int ops) {
		StringBuilder sb = new StringBuilder();
		if ((ops & OP_POSTPONED) != 0)
			sb.append("OP_POSTPONED ");
		if ((ops & SelectionKey.OP_WRITE) != 0)
			sb.append("OP_WRITE ");
		if ((ops & SelectionKey.OP_READ) != 0)
			sb.append("OP_READ ");
		return sb.toString();
	}
}
