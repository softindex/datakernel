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

package io.datakernel.serial.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

/**
 * Represent the TCP connection which  processes received items with {@link StreamProducer} and {@link StreamConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public final class SocketStreamingConnection implements SocketStreaming, AsyncTcpSocket.EventHandler {
	private final AsyncTcpSocket asyncTcpSocket;

	private SocketStreamProducer socketReader;
	private SocketStreamConsumer socketWriter;

	// region creators
	private SocketStreamingConnection(AsyncTcpSocket asyncTcpSocket) {
		this.asyncTcpSocket = asyncTcpSocket;
	}

	public static SocketStreamingConnection create(AsyncTcpSocket asyncTcpSocket) {
		return new SocketStreamingConnection(asyncTcpSocket);
	}
	// endregion

	@Override
	public SerialSupplier<ByteBuf> getSocketReader() {
		if (socketReader == null) {
			socketReader = new SocketStreamProducer(asyncTcpSocket);
			socketReader.getEndOfStream()
					.whenException(this::onClosedWithError);
			asyncTcpSocket.read(); // prefetch
		}
		return socketReader;
	}

	@Override
	public SerialConsumer<ByteBuf> getSocketWriter() {
		if (socketWriter == null) {
			socketWriter = new SocketStreamConsumer(asyncTcpSocket);
			socketWriter.getEndOfStream()
					.whenException(this::onClosedWithError);
		}
		return socketWriter;
	}

	/**
	 * Is called after connection registration. Wires socketReader with StreamConsumer specified by,
	 * and socketWriter with StreamProducer, that are specified by overridden method {@code wire} of subclass.
	 * If StreamConsumer is null, items from socketReader are ignored. If StreamProducer is null, socketWriter
	 * gets EndOfStream signal.
	 */
	@Override
	public void onRegistered() {
	}

	/**
	 * Sends received bytes to StreamConsumer
	 */
	@Override
	public void onRead(ByteBuf buf) {
		if (socketReader != null) socketReader.onRead(buf);
		closeIfDone();
	}

	@Override
	public void onReadEndOfStream() {
		if (socketReader != null) socketReader.onReadEndOfStream();
		closeIfDone();
	}

	@Override
	public void onWrite() {
		if (socketWriter != null) socketWriter.onWrite();
		closeIfDone();
	}

	private void closeIfDone() {
		if ((socketReader == null || socketReader.isClosed()) && (socketWriter == null || socketWriter.isClosed())) {
			asyncTcpSocket.close();
		}
	}

	@Override
	public void onClosedWithError(Throwable e) {
		asyncTcpSocket.close();
		if (socketReader != null) socketReader.closeWithError(e);
		if (socketWriter != null) socketWriter.closeWithError(e);
	}

	@Override
	public String toString() {
		return "{" + asyncTcpSocket + "}";
	}
}
