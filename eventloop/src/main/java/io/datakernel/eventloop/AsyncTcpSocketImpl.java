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
import io.datakernel.exception.SimpleException;
import io.datakernel.net.SocketSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;

import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings({"WeakerAccess", "AssertWithSideEffects"})
public final class AsyncTcpSocketImpl implements AsyncTcpSocket, NioChannelEventHandler {
	public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 16 * 1024;
	public static final int OP_POSTPONED = 1 << 7;  // SelectionKey constant
	private static final int MAX_MERGE_SIZE = 16 * 1024;

	@SuppressWarnings("ThrowableInstanceNeverThrown")
	public static final SimpleException TIMEOUT_EXCEPTION = new SimpleException("timed out");
	public static final int NO_TIMEOUT = -1;

	private final Eventloop eventloop;
	private final SocketChannel channel;
	private final ArrayDeque<ByteBuf> writeQueue = new ArrayDeque<>();
	private boolean writeEndOfStream;
	private EventHandler socketEventHandler;
	private SelectionKey key;

	private int ops = 0;
	private boolean writing = false;

	private long readTimeout = NO_TIMEOUT;
	private long writeTimeout = NO_TIMEOUT;

	private ScheduledRunnable checkReadTimeout = new ScheduledRunnable() {
		@Override
		public void run() {
			closeWithError(TIMEOUT_EXCEPTION, false);
		}
	};

	private ScheduledRunnable checkWriteTimeout = new ScheduledRunnable() {
		@Override
		public void run() {
			closeWithError(TIMEOUT_EXCEPTION, false);
		}
	};

	private AsyncTcpSocketContract contractChecker;

	protected int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;

	private final Runnable writeRunnable = new Runnable() {
		@Override
		public void run() {
			if (!writing || !isOpen())
				return;
			writing = false;
			try {
				doWrite();
			} catch (IOException e) {
				closeWithError(e, true);
			}
		}
	};

	// region builders
	public static AsyncTcpSocketImpl wrapChannel(Eventloop eventloop, SocketChannel socketChannel,
	                                             SocketSettings socketSettings) {
		try {
			socketSettings.applySettings(socketChannel);
		} catch (IOException e) {
			throw new AssertionError("Failed to apply socketSettings", e);
		}
		AsyncTcpSocketImpl asyncTcpSocket = new AsyncTcpSocketImpl(eventloop, socketChannel);
		socketSettings.applyReadWriteTimeoutsTo(asyncTcpSocket);
		return asyncTcpSocket;
	}

	public static AsyncTcpSocketImpl wrapChannel(Eventloop eventloop, SocketChannel socketChannel) {
		return new AsyncTcpSocketImpl(eventloop, socketChannel);
	}

	private AsyncTcpSocketImpl(Eventloop eventloop, SocketChannel socketChannel) {
		this.eventloop = checkNotNull(eventloop);
		this.channel = checkNotNull(socketChannel);

		// jmx
		eventloop.getStats().recordSocketCreateEvent();

		assert (this.contractChecker = AsyncTcpSocketContract.create()) != null;
	}
	// endregion

	@Override
	public void setEventHandler(EventHandler eventHandler) {
		this.socketEventHandler = eventHandler;
	}

	public AsyncTcpSocketImpl readTimeout(long readTimeout) {
		this.readTimeout = readTimeout;
		return this;
	}

	public AsyncTcpSocketImpl writeTimeout(long writeTimeout) {
		this.writeTimeout = writeTimeout;
		return this;
	}

	public final void register() {
		socketEventHandler.onRegistered();
		try {
			key = channel.register(eventloop.ensureSelector(), ops, this);
		} catch (final IOException e) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					closeChannel();
					assert contractChecker.onClosedWithError();
					socketEventHandler.onClosedWithError(e);
				}
			});
		}
	}

	// timeouts management
	void scheduleReadTimeOut() {
		if (!checkReadTimeout.isScheduledNow()) {
			long roundedTimestamp = ceilMod(eventloop.currentTimeMillis() + readTimeout, 100);
			eventloop.scheduleBackground(roundedTimestamp, checkReadTimeout);
		}
	}

	void scheduleWriteTimeOut() {
		if (!checkWriteTimeout.isScheduledNow()) {
			long roundedTimestamp = ceilMod(eventloop.currentTimeMillis() + writeTimeout, 100);
			eventloop.scheduleBackground(roundedTimestamp, checkWriteTimeout);
		}
	}

	private static long ceilMod(long value, long modulo) {
		long remainder = value % modulo;
		return remainder == 0 ? value : value - remainder + modulo;
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

	// read cycle
	@Override
	public void read() {
		assert contractChecker.read();

		if (readTimeout != NO_TIMEOUT) {
			scheduleReadTimeOut();
		}
		readInterest(true);
	}

	@Override
	public void onReadReady() {
		int oldOps = ops;
		ops = ops | OP_POSTPONED;
		readInterest(false);
		if (checkReadTimeout.isScheduledNow()) {
			checkReadTimeout.cancel();
		}
		doRead();
		int newOps = ops & ~OP_POSTPONED;
		ops = oldOps;
		interests(newOps);
	}

	private void doRead() {
		ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
		ByteBuffer buffer = buf.toWriteByteBuffer();

		int numRead;
		try {
			numRead = channel.read(buffer);
			buf.ofWriteByteBuffer(buffer);
		} catch (IOException e) {
			buf.recycle();
			closeWithError(e, false);
			return;
		}

		if (numRead == 0) {
			buf.recycle();
			return;
		}

		if (numRead == -1) {
			buf.recycle();
			assert contractChecker.onReadEndOfStream();
			socketEventHandler.onReadEndOfStream();
			return;
		}

		assert contractChecker.onRead();
		socketEventHandler.onRead(buf);
	}

	// write cycle
	@Override
	public void write(ByteBuf buf) {
		assert contractChecker.write();
		assert eventloop.inEventloopThread();
		if (writeTimeout != NO_TIMEOUT) {
			scheduleWriteTimeOut();
		}
		writeQueue.add(buf);
		postWriteRunnable();
	}

	@Override
	public void writeEndOfStream() {
		assert contractChecker.writeEndOfStream();
		assert eventloop.inEventloopThread();
		if (writeEndOfStream) return;
		writeEndOfStream = true;
		postWriteRunnable();
	}

	@Override
	public void onWriteReady() {
		writing = false;
		try {
			doWrite();
		} catch (IOException e) {
			closeWithError(e, false);
		}
	}

	private void doWrite() throws IOException {
		while (true) {
			ByteBuf bufToSend = writeQueue.poll();
			if (bufToSend == null)
				break;

			while (true) {
				ByteBuf nextBuf = writeQueue.peek();
				if (nextBuf == null)
					break;

				int bytesToCopy = nextBuf.readRemaining(); // bytes to append to bufToSend
				if (bufToSend.readPosition() + bufToSend.readRemaining() + bytesToCopy > bufToSend.array().length)
					bytesToCopy += bufToSend.readRemaining(); // append will resize bufToSend
				if (bytesToCopy < MAX_MERGE_SIZE) {
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
				bufToSend.recycle();
				throw e;
			}

			bufToSend.ofReadByteBuffer(bufferToSend);

			if (bufToSend.canRead()) {
				writeQueue.addFirst(bufToSend); // put the buf back to the queue, to send it the next time
				break;
			}
			bufToSend.recycle();
		}

		if (writeQueue.isEmpty()) {
			if (checkWriteTimeout.isScheduledNow()) {
				checkWriteTimeout.cancel();
			}
			if (writeEndOfStream) {
				channel.shutdownOutput();
			}
			writeInterest(false);
			assert contractChecker.onWrite();
			socketEventHandler.onWrite();
		} else {
			writeInterest(true);
		}
	}

	// close methods
	@Override
	public void close() {
		assert contractChecker.close();
		assert eventloop.inEventloopThread();
		if (key == null) return;
		closeChannel();
		key = null;
		for (ByteBuf buf : writeQueue) {
			buf.recycle();
		}
		writeQueue.clear();
		if (checkWriteTimeout.isScheduledNow()) {
			checkWriteTimeout.cancel();
		}
		if (checkReadTimeout.isScheduledNow()) {
			checkReadTimeout.cancel();
		}
	}

	private void closeChannel() {
		if (channel == null) return;
		try {
			channel.close();
		} catch (IOException e) {
			eventloop.recordIoError(e, toString());
		}

		// jmx
		eventloop.getStats().recordSocketCloseEvent();
	}

	private void closeWithError(final Exception e, boolean fireAsync) {
		if (isOpen()) {
			assert contractChecker.closeAndNotifyEventHandler();
			close();
			if (fireAsync)
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						assert contractChecker.onClosedWithError();
						socketEventHandler.onClosedWithError(e);
					}
				});
			else {
				assert contractChecker.onClosedWithError();
				socketEventHandler.onClosedWithError(e);
			}
		}
	}

	// miscellaneous
	private void postWriteRunnable() {
		if (!writing) {
			writing = true;
			eventloop.post(writeRunnable);
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
				", writing=" + writing +
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
