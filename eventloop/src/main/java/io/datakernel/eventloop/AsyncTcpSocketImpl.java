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
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Recyclable.deepRecycle;

@SuppressWarnings({"WeakerAccess", "AssertWithSideEffects"})
public final class AsyncTcpSocketImpl implements AsyncTcpSocket, NioChannelEventHandler {
	public static final MemSize DEFAULT_READ_BUF_SIZE = MemSize.kilobytes(16);

	@SuppressWarnings("ThrowableInstanceNeverThrown")
	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException("timed out");
	public static final int NO_TIMEOUT = -1;

	private static final MemSize MAX_MERGE_SIZE = MemSize.kilobytes(16);
	private static final AtomicInteger connectionCount = new AtomicInteger(0);

	private final Eventloop eventloop;
	private SocketChannel channel;
	private final ArrayDeque<ByteBuf> readQueue = new ArrayDeque<>();
	private boolean readEndOfStream;
	private final ArrayDeque<ByteBuf> writeQueue = new ArrayDeque<>();
	private boolean writeEndOfStream;

	private SettableStage<Void> write;
	private SettableStage<ByteBuf> read;

	@Nullable
	private SelectionKey key;

	private boolean reentrantCall;
	private int ops = 0;
//	private long readTimestamp = 0L;
//	private long writeTimestamp = 0L;

	private long readTimeout = NO_TIMEOUT;
	private long writeTimeout = NO_TIMEOUT;
	protected int readMaxSize = DEFAULT_READ_BUF_SIZE.toInt();
	protected int writeMaxSize = MAX_MERGE_SIZE.toInt();

	@Nullable
	private ScheduledRunnable scheduledReadTimeout;

	@Nullable
	private ScheduledRunnable scheduledWriteTimeout;

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
			asyncTcpSocket.readMaxSize = socketSettings.getImplReadSize().toInt();
		if (socketSettings.hasImplWriteSize())
			asyncTcpSocket.writeMaxSize = socketSettings.getImplWriteSize().toInt();
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

	public static int getConnectionCount() {
		return connectionCount.get();
	}

	// timeouts management
	private void scheduleReadTimeout() {
		if (scheduledReadTimeout == null) {
			scheduledReadTimeout = eventloop.delayBackground(readTimeout, () -> {
				if (inspector != null) inspector.onReadTimeout();
				scheduledReadTimeout = null;
				closeWithError(TIMEOUT_EXCEPTION);
			});
		}
	}

	private void scheduleWriteTimeout() {
		if (scheduledWriteTimeout == null) {
			scheduledWriteTimeout = eventloop.delayBackground(writeTimeout, () -> {
				if (inspector != null) inspector.onWriteTimeout();
				scheduledWriteTimeout = null;
				closeWithError(TIMEOUT_EXCEPTION);
			});
		}
	}

	private void updateInterests() {
		if (reentrantCall || !isOpen()) return;
		int newOps = (read != null ? SelectionKey.OP_READ : 0) + (write != null ? SelectionKey.OP_WRITE : 0);
		if (key == null) {
			ops = newOps;
			doRegister();
			if (read != null) {
				doRead();
			}
		} else {
			if (ops != newOps) {
				ops = newOps;
				key.interestOps(ops);
			}
		}
	}

	private void doRegister() {
		try {
			key = channel.register(eventloop.ensureSelector(), ops, this);
			connectionCount.incrementAndGet();
		} catch (IOException e) {
			closeWithError(e);
		}
	}

	@Override
	public Stage<ByteBuf> read() {
		if (read != null) return read;
		if (!readQueue.isEmpty() || readEndOfStream) return Stage.of(readQueue.poll());
		read = new SettableStage<>();
		if (readTimeout != NO_TIMEOUT) {
			scheduleReadTimeout();
		}
		updateInterests();
		return read;
	}

	@Override
	public void onReadReady() {
		reentrantCall = true;
		doRead();
		if (!readQueue.isEmpty() || readEndOfStream) {
			SettableStage<ByteBuf> read = this.read;
			this.read = null;
			read.set(readQueue.poll());
			closeIfDone();
		}
		reentrantCall = false;
		updateInterests();
	}

	private void doRead() {
		while (true) {
			ByteBuf buf = ByteBufPool.allocate(readMaxSize);
			ByteBuffer buffer = buf.toWriteByteBuffer();

			int numRead;
			try {
				numRead = channel.read(buffer);
				buf.ofWriteByteBuffer(buffer);
			} catch (IOException e) {
				buf.recycle();
				if (inspector != null) inspector.onReadError(e);
				closeWithError(e);
				return;
			}

			if (numRead == 0) {
				if (inspector != null) inspector.onRead(buf);
				buf.recycle();
				return;
			}

			if (scheduledReadTimeout != null) {
				scheduledReadTimeout.cancel();
				scheduledReadTimeout = null;
			}

			if (numRead == -1) {
				buf.recycle();
				if (inspector != null) inspector.onReadEndOfStream();
				readEndOfStream = true;
				return;
			}

			if (inspector != null) inspector.onRead(buf);
			readQueue.add(buf);

			if (buf.writeRemaining() != 0) {
				return;
			}
		}
	}

	// write cycle
	@Override
	public Stage<Void> write(@Nullable ByteBuf buf) {
		assert eventloop.inEventloopThread();
		assert !writeEndOfStream;
		if (buf != null) {
			writeQueue.add(buf);
		} else {
			writeEndOfStream = true;
		}
		if (write != null) return write;
		try {
			if (doWrite()) {
				closeIfDone();
				return Stage.complete();
			}
		} catch (IOException e) {
			closeWithError(e);
			return Stage.ofException(e);
		}
		write = new SettableStage<>();
		if (writeTimeout != NO_TIMEOUT) {
			scheduleWriteTimeout();
		}
		updateInterests();
		return write;
	}

	@Override
	public void onWriteReady() {
		assert write != null;
		reentrantCall = true;
		try {
			if (doWrite()) {
				SettableStage<Void> write = this.write;
				this.write = null;
				write.set(null);
				closeIfDone();
			}
		} catch (IOException e) {
			closeWithError(e);
		}
		reentrantCall = false;
		updateInterests();
	}

	private boolean doWrite() throws IOException {
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
			if (scheduledWriteTimeout != null) {
				scheduledWriteTimeout.cancel();
				scheduledWriteTimeout = null;
			}
			if (writeEndOfStream) {
				channel.shutdownOutput();
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void close() {
		closeWithError(CHANNEL_CLOSED);
	}

	@Override
	public void closeWithError(@Nullable Throwable e) {
		assert eventloop.inEventloopThread();
		if (channel == null) return;
		eventloop.closeChannel(channel);
		channel = null;
		key = null;
		connectionCount.decrementAndGet();
		deepRecycle(readQueue);
		deepRecycle(writeQueue);
		if (scheduledWriteTimeout != null) {
			scheduledWriteTimeout.cancel();
			scheduledWriteTimeout = null;
		}
		if (scheduledReadTimeout != null) {
			scheduledReadTimeout.cancel();
			scheduledReadTimeout = null;
		}
		if (write != null) {
			write.setException(e);
			write = null;
		}
		if (read != null) {
			read.setException(e);
			read = null;
		}
	}

	private void closeIfDone() {
		if (readEndOfStream && writeEndOfStream && read == null && write == null) {
			close();
		}
	}

	public boolean isOpen() {
		return channel != null;
	}

	public SocketChannel getSocketChannel() {
		return channel;
	}

	@Override
	public String toString() {
		return "AsyncTcpSocketImpl{" +
				"channel=" + (channel != null ? channel : "") +
				", writeQueueSize=" + writeQueue.size() +
				", writeEndOfStream=" + writeEndOfStream +
				", read=" + read +
				", write=" + write +
				", processing=" + reentrantCall +
				"}";
	}
}
