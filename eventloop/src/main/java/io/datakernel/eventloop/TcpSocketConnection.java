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
import io.datakernel.bytebuf.ByteBufQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;

/**
 * Represent the TCP connection, which is {@link SocketConnection}. It is created with socketChannel
 * and in which sides can exchange {@link ByteBuf}.
 */
public abstract class TcpSocketConnection extends SocketConnection {
	protected final SocketChannel channel;
	protected final InetSocketAddress remoteSocketAddress;
	protected final ByteBufQueue writeQueue;
	protected final ByteBufQueue readQueue;

	/**
	 * Creates a new instance of TcpSocketConnection
	 *
	 * @param eventloop     eventloop in which this connection will be handled
	 * @param socketChannel socketChannel for creating this connection
	 */
	public TcpSocketConnection(Eventloop eventloop, SocketChannel socketChannel) {
		super(eventloop);
		this.channel = socketChannel;
		try {
			this.remoteSocketAddress = (InetSocketAddress) channel.getRemoteAddress();
		} catch (IOException ignored) {
			throw new AssertionError("I/O error occurs or channel closed");
		}
		this.writeQueue = new ByteBufQueue();
		this.readQueue = new ByteBufQueue();
	}

	/**
	 * Reads received bytes, creates ByteBufs with it and call its method onRead() with
	 * this buffer.
	 */
	@Override
	public void onReadReady() {
		ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
		ByteBuffer byteBuffer = buf.toByteBuffer();

		int numRead;
		try {
			numRead = channel.read(byteBuffer);
			buf.setByteBuffer(byteBuffer);
		} catch (IOException e) {
			buf.recycle();
			onReadException(e);
			return;
		}

		if (numRead == 0) {
			buf.recycle();
			return;
		}

		if (numRead == -1) {
			buf.recycle();
			onReadEndOfStream();
			if (isRegistered()) {
				readInterest(false); // prevent spinning if connection is still open
			}
			return;
		}

		if (numRead > 0) {
			readTime = eventloop.currentTimeMillis();
		}

		buf.flip();
		onRead(buf);
	}

	protected void onRead(ByteBuf buf) {
		readQueue.add(buf);
		onRead();
	}

	/**
	 * It processes received ByteBufs
	 * These ByteBufs are in readQueue at the moment of working this function.
	 */
	protected abstract void onRead();

	/**
	 * Peeks ByteBuf from writeQueue, and sends its bytes to address.
	 */
	private void doWrite() {
		boolean wasWritten = false;

		while (!writeQueue.isEmpty()) {
			ByteBuf buf = writeQueue.peekBuf();
			ByteBuffer byteBuffer = buf.toByteBuffer();
			int remainingOld = buf.remaining();
			try {
				channelWrite(byteBuffer);
				buf.setByteBuffer(byteBuffer);
			} catch (IOException e) {
				onWriteException(e);
				return;
			}

			int remainingNew = buf.remaining();
			if (remainingNew != remainingOld) {
				wasWritten = true;
			}

			if (remainingNew > 0) {
				break;
			}
			writeQueue.take();
			buf.recycle();
		}

		if (wasWritten) {
			writeTime = eventloop.currentTimeMillis();
		}

		if (writeQueue.isEmpty()) {
			onWriteFlushed();
			writeInterest(false);
		} else {
			writeInterest(true);
		}
	}

	protected void write(ByteBuf buf) {
		if (writeQueue.isEmpty()) {
			writeQueue.add(buf);
			doWrite();
		} else {
			writeQueue.add(buf);
		}
	}

	protected int channelWrite(ByteBuffer byteBuffer) throws IOException {
		return channel.write(byteBuffer);
	}

	/**
	 * This method is called if writeInterest is on and it is possible to write to the channel.
	 */
	@Override
	public void onWriteReady() {
		doWrite();
	}

	/**
	 * Before closing this connection it clears readQueue and writeQueue.
	 */
	@Override
	public void onClosed() {
		readQueue.clear();
		writeQueue.clear();
	}

	@Override
	public final SelectableChannel getChannel() {
		return this.channel;
	}

	protected void shutdownInput() throws IOException {
		channel.shutdownInput();
	}

	protected void shutdownOutput() throws IOException {
		channel.shutdownOutput();
	}

	@Nullable
	public InetSocketAddress getRemoteSocketAddress() {
		return remoteSocketAddress;
	}

	public String getChannelInfo() {
		return channel.toString();
	}

	@Override
	protected String getDebugName() {
		return super.getDebugName() + "(" + remoteSocketAddress + ")";
	}
}
