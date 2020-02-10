/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.exception.AsyncTimeoutException;
import io.datakernel.common.inspector.AbstractInspector;
import io.datakernel.common.inspector.BaseInspector;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioChannelEventHandler;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.jmx.ValueStats;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.common.MemSize.kilobytes;
import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.common.Utils.nullify;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

@SuppressWarnings("WeakerAccess")
public final class AsyncTcpSocketNio implements AsyncTcpSocket, NioChannelEventHandler {
	public static final int DEFAULT_READ_BUFFER_SIZE = ApplicationSettings.getMemSize(AsyncTcpSocketNio.class, "readBufferSize", kilobytes(16)).toInt();

	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException(AsyncTcpSocketNio.class, "timed out");
	public static final int NO_TIMEOUT = 0;

	private static final AtomicInteger CONNECTION_COUNT = new AtomicInteger(0);

	private final Eventloop eventloop;
	@Nullable
	private SocketChannel channel;
	@Nullable
	private ByteBuf readBuf;
	private boolean readEndOfStream;
	@Nullable
	private ByteBuf writeBuf;
	private boolean writeEndOfStream;

	@Nullable
	private SettablePromise<ByteBuf> read;
	@Nullable
	private SettablePromise<Void> write;

	private SelectionKey key;
	private byte ops;

	private int readTimeout = NO_TIMEOUT;
	private int writeTimeout = NO_TIMEOUT;
	private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;

	@Nullable
	private ScheduledRunnable scheduledReadTimeout;
	@Nullable
	private ScheduledRunnable scheduledWriteTimeout;

	@Nullable
	private Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onReadTimeout();

		void onRead(ByteBuf buf);

		void onReadEndOfStream();

		void onReadError(IOException e);

		void onWriteTimeout();

		void onWrite(ByteBuf buf, int bytes);

		void onWriteError(IOException e);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
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

	public static AsyncTcpSocketNio wrapChannel(Eventloop eventloop, SocketChannel socketChannel, @Nullable SocketSettings socketSettings) {
		AsyncTcpSocketNio asyncTcpSocket = new AsyncTcpSocketNio(eventloop, socketChannel);
		if (socketSettings == null) return asyncTcpSocket;
		try {
			socketSettings.applySettings(socketChannel);
		} catch (IOException ignored) {
		}
		if (socketSettings.hasImplReadTimeout()) {
			asyncTcpSocket.readTimeout = (int) socketSettings.getImplReadTimeoutMillis();
		}
		if (socketSettings.hasImplWriteTimeout()) {
			asyncTcpSocket.writeTimeout = (int) socketSettings.getImplWriteTimeoutMillis();
		}
		if (socketSettings.hasReadBufferSize()) {
			asyncTcpSocket.readBufferSize = socketSettings.getImplReadBufferSizeBytes();
		}
		return asyncTcpSocket;
	}

	public static Promise<AsyncTcpSocketNio> connect(InetSocketAddress address) {
		return connect(address, null, null);
	}

	public static Promise<AsyncTcpSocketNio> connect(InetSocketAddress address, @Nullable Duration duration, @Nullable SocketSettings socketSettings) {
		return connect(address, duration == null ? 0 : duration.toMillis(), socketSettings);
	}

	public static Promise<AsyncTcpSocketNio> connect(InetSocketAddress address, long timeout, @Nullable SocketSettings socketSettings) {
		Eventloop eventloop = getCurrentEventloop();
		return Promise.<SocketChannel>ofCallback(cb -> eventloop.connect(address, timeout, cb))
				.map(channel -> wrapChannel(eventloop, channel, socketSettings));
	}

	public AsyncTcpSocketNio withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	private AsyncTcpSocketNio(Eventloop eventloop, @NotNull SocketChannel socketChannel) {
		this.eventloop = eventloop;
		this.channel = socketChannel;
	}
	// endregion

	public static int getConnectionCount() {
		return CONNECTION_COUNT.get();
	}

	// timeouts management
	private void scheduleReadTimeout() {
		assert scheduledReadTimeout == null && readTimeout != NO_TIMEOUT;
		scheduledReadTimeout = eventloop.delayBackground(readTimeout, wrapContext(this, () -> {
			if (inspector != null) inspector.onReadTimeout();
			scheduledReadTimeout = null;
			close(TIMEOUT_EXCEPTION);
		}));
	}

	private void scheduleWriteTimeout() {
		assert scheduledWriteTimeout == null && writeTimeout != NO_TIMEOUT;
		scheduledWriteTimeout = eventloop.delayBackground(writeTimeout, wrapContext(this, () -> {
			if (inspector != null) inspector.onWriteTimeout();
			scheduledWriteTimeout = null;
			close(TIMEOUT_EXCEPTION);
		}));
	}

	private void updateInterests() {
		assert isOpen() && ops >= 0;
		byte newOps = (byte) (((readBuf == null && !readEndOfStream) ? SelectionKey.OP_READ : 0) | (writeBuf == null || writeEndOfStream ? 0 : SelectionKey.OP_WRITE));
		if (key == null) {
			ops = newOps;
			try {
				key = channel.register(eventloop.ensureSelector(), ops, this);
				CONNECTION_COUNT.incrementAndGet();
			} catch (ClosedChannelException e) {
				close(e);
			}
		} else {
			if (ops != newOps) {
				ops = newOps;
				key.interestOps(ops);
			}
		}
	}

	@NotNull
	@Override
	public Promise<ByteBuf> read() {
		if (channel == null) return Promise.ofException(CLOSE_EXCEPTION);
		read = null;
		if (readBuf != null || readEndOfStream) {
			ByteBuf readBuf = this.readBuf;
			this.readBuf = null;
			return Promise.of(readBuf);
		}
		SettablePromise<ByteBuf> read = new SettablePromise<>();
		this.read = read;
		if (scheduledReadTimeout == null && readTimeout != NO_TIMEOUT) {
			scheduleReadTimeout();
		}
		if (ops >= 0) {
			updateInterests();
		}
		return read;
	}

	@Override
	public void onReadReady() {
		ops = (byte) (ops | 0x80);
		try {
			doRead();
		} catch (IOException e) {
			close(e);
			return;
		}
		if (read != null && (readBuf != null || readEndOfStream)) {
			SettablePromise<@Nullable ByteBuf> read = this.read;
			ByteBuf readBuf = this.readBuf;
			this.read = null;
			this.readBuf = null;
			read.set(readBuf);
		}
		if (!isOpen()) return;
		ops = (byte) (ops & 0x7f);
		updateInterests();
	}

	private void doRead() throws IOException {
		assert channel != null;
		ByteBuf buf = ByteBufPool.allocate(readBufferSize);
		ByteBuffer buffer = buf.toWriteByteBuffer();

		int numRead;
		try {
			numRead = channel.read(buffer);
			buf.ofWriteByteBuffer(buffer);
		} catch (IOException e) {
			buf.recycle();
			if (inspector != null) inspector.onReadError(e);
			throw e;
		}

		if (numRead == 0) {
			if (inspector != null) inspector.onRead(buf);
			buf.recycle();
			return;
		}

		scheduledReadTimeout = nullify(scheduledReadTimeout, ScheduledRunnable::cancel);

		if (numRead == -1) {
			buf.recycle();
			if (inspector != null) inspector.onReadEndOfStream();
			readEndOfStream = true;
			if (writeEndOfStream && writeBuf == null) {
				doClose();
			}
			return;
		}

		if (inspector != null) inspector.onRead(buf);

		if (readBuf == null) {
			readBuf = buf;
		} else {
			readBuf = ByteBufPool.ensureWriteRemaining(readBuf, buf.readRemaining());
			readBuf.put(buf.array(), buf.head(), buf.readRemaining());
			buf.recycle();
		}
	}

	// write cycle
	@NotNull
	@Override
	public Promise<Void> write(@Nullable ByteBuf buf) {
		assert eventloop.inEventloopThread();
		checkState(!writeEndOfStream, "End of stream has already been sent");
		if (channel == null) {
			if (buf != null) buf.recycle();
			return Promise.ofException(CLOSE_EXCEPTION);
		}
		writeEndOfStream |= buf == null;

		if (writeBuf == null) {
			if (buf != null && !buf.canRead()) {
				return Promise.complete();
			}
			writeBuf = buf;
		} else {
			if (buf != null) {
				writeBuf = ByteBufPool.ensureWriteRemaining(this.writeBuf, buf.readRemaining());
				writeBuf.put(buf.array(), buf.head(), buf.readRemaining());
				buf.recycle();
			}
		}

		if (write != null) return write;

		try {
			doWrite();
		} catch (IOException e) {
			close(e);
			return Promise.ofException(e);
		}

		if (this.writeBuf == null) {
			return Promise.complete();
		}
		SettablePromise<Void> write = new SettablePromise<>();
		this.write = write;
		if (scheduledWriteTimeout == null && writeTimeout != NO_TIMEOUT) {
			scheduleWriteTimeout();
		}
		if (ops >= 0) {
			updateInterests();
		}
		return write;
	}

	@Override
	public void onWriteReady() {
		assert write != null;
		ops = (byte) (ops | 0x80);
		try {
			doWrite();
		} catch (IOException e) {
			close(e);
			return;
		}
		if (writeBuf == null) {
			SettablePromise<@Nullable Void> write = this.write;
			this.write = null;
			write.set(null);
		}
		if (!isOpen()) return;
		ops = (byte) (ops & 0x7f);
		updateInterests();
	}

	private void doWrite() throws IOException {
		assert channel != null;
		if (writeBuf != null) {
			ByteBuf buf = this.writeBuf;
			ByteBuffer buffer = buf.toReadByteBuffer();

			try {
				channel.write(buffer);
			} catch (IOException e) {
				if (inspector != null) inspector.onWriteError(e);
				throw e;
			}

			if (inspector != null) inspector.onWrite(buf, buffer.position() - buf.head());

			buf.ofReadByteBuffer(buffer);

			if (buf.canRead()) {
				return;
			} else {
				buf.recycle();
				writeBuf = null;
			}
		}

		scheduledWriteTimeout = nullify(scheduledWriteTimeout, ScheduledRunnable::cancel);

		if (writeEndOfStream) {
			if (readEndOfStream) {
				doClose();
			} else {
				channel.shutdownOutput();
			}
		}
	}

	@Override
	public void close(@NotNull Throwable e) {
		assert eventloop.inEventloopThread();
		if (channel == null) return;
		doClose();
		readBuf = nullify(readBuf, ByteBuf::recycle);
		writeBuf = nullify(writeBuf, ByteBuf::recycle);
		scheduledReadTimeout = nullify(scheduledReadTimeout, ScheduledRunnable::cancel);
		scheduledWriteTimeout = nullify(scheduledWriteTimeout, ScheduledRunnable::cancel);
		read = nullify(read, SettablePromise::setException, e);
		write = nullify(write, SettablePromise::setException, e);
	}

	private void doClose() {
		eventloop.closeChannel(channel, key);
		channel = null;
		CONNECTION_COUNT.decrementAndGet();
	}

	public boolean isOpen() {
		return channel != null;
	}

	@Nullable
	public SocketChannel getSocketChannel() {
		return channel;
	}

	@Override
	public String toString() {
		return "AsyncTcpSocketImpl{" +
				"channel=" + (channel != null ? channel : "") +
				", readBuf=" + readBuf +
				", writeBuf=" + writeBuf +
				", readEndOfStream=" + readEndOfStream +
				", writeEndOfStream=" + writeEndOfStream +
				", read=" + read +
				", write=" + write +
				", ops=" + ops +
				"}";
	}
}
