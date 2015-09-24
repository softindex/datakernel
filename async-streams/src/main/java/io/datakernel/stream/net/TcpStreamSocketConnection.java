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

package io.datakernel.stream.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.TcpSocketConnection;
import io.datakernel.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;

/**
 * Represent the TCP connection which  processes received items with {@link StreamProducer} and {@link StreamConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public abstract class TcpStreamSocketConnection extends TcpSocketConnection {
	private static final Logger logger = LoggerFactory.getLogger(TcpStreamSocketConnection.class);

	protected final class Reader extends AbstractStreamProducer<ByteBuf> {
		public Reader(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onStarted() {
		}

		@Override
		protected void onDataReceiverChanged() {
		}

		@Override
		protected void onSuspended() {
			readInterest(false);
		}

		@Override
		protected void onResumed() {
			readInterest(true);
		}

		@Override
		protected void onError(Exception e) {
			TcpStreamSocketConnection.this.onInternalException(e);
		}

		@Override
		public void send(ByteBuf item) {
			super.send(item);
		}

		@Override
		public void sendEndOfStream() {
			super.sendEndOfStream();
		}

		@Override
		public void closeWithError(Exception e) {
			super.closeWithError(e);
		}

		//		@Override
//		protected void onClosed() {
//			closeIfDone();
//		}

	}

	protected final class Writer extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
		public Writer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onStarted() {
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return this;
		}

		@Override
		public void onEndOfStream() {
			if (writeQueue.isEmpty()) {
//				closeUpstream();
				closeIfDone();
			}
		}

		@Override
		protected void onError(Exception e) {
			onInternalException(e);
		}

		/**
		 * Method which is called after each receiving result
		 *
		 * @param buf received item
		 */
		@Override
		public void onData(ByteBuf buf) {
			write(buf);
			if (writeQueue.isEmpty()) {
				resume();
			} else {
				suspend();
			}
		}

		@Override
		public void suspend() {
			super.suspend();
		}

		@Override
		public void resume() {
			super.resume();
		}

		@Override
		public void close() {
			super.close();
		}

		@Override
		public void closeWithError(Exception e) {
			super.closeWithError(e);
		}
	}

	public static final int DEFAULT_STREAM_BUFFER_SIZE = 256 * 1024;

	protected final Reader socketReader;
	protected final Writer socketWriter;

	private String name;

	/**
	 * Returns new instance of TcpStreamSocketConnection
	 *
	 * @param eventloop     eventloop in with this connection will be handled
	 * @param socketChannel socketChannel for this connection
	 */
	public TcpStreamSocketConnection(NioEventloop eventloop, SocketChannel socketChannel) {
		super(eventloop, socketChannel);
		this.receiveBufferSize = DEFAULT_STREAM_BUFFER_SIZE;
		this.socketReader = new Reader(eventloop);
		this.socketWriter = new Writer(eventloop);
	}

	/**
	 * Is called after connection registration. Wires socketReader with StreamConsumer specified by,
	 * and socketWriter with StreamProducer, that are specified by overridden method {@code wire} of subclass.
	 * If StreamConsumer is null, items from socketReader are ignored. If StreamProducer is null, socketWriter
	 * gets EndOfStream signal.
	 */
	@Override
	public void onRegistered() {
		wire(socketReader, socketWriter);
		if (socketReader.getDownstream() == null)
			socketReader.streamTo(new StreamConsumers.Closing<ByteBuf>(eventloop));
		if (socketWriter.getUpstream() == null)
			new StreamProducers.EndOfStream<ByteBuf>(eventloop).streamTo(socketWriter);
	}

	/**
	 * Method should wire socketReader with appropriate {@link StreamConsumer} and socketWriter with appropriate
	 * {@link StreamProducer}.
	 * However one of them could be null. If both {@link StreamConsumer} and {@link StreamProducer} are null
	 * socket connection just won't do any work.
	 *
	 * @param socketReader producer that reads ByteBufs from socket and send them
	 *                     to StreamConsumer specified in overridden method in subclass
	 * @param socketWriter consumer that receive ByteBufs from StreamProducer specified in overridden method
	 *                     in subclass and writes them to socket
	 */
	protected abstract void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter);

	@Override
	protected void onReadEndOfStream() {
		logger.trace("onReadEndOfStream for {}", this);
		socketReader.sendEndOfStream();
	}

	private void closeIfDone() {
		if (!isRegistered())
			return;
		if (socketWriter.getStatus() >= AbstractStreamConsumer.CLOSED) {
			logger.trace("done, closing {}", this);
			close();
		}
//		if (socketReader.getStatus() >= AbstractStreamProducer.CLOSED) {
//			try {
//				channel.shutdownInput();
//			} catch (IOException e) {
//				logger.error("shutdownInput error {} for {}", e.toString(), this);
//			}
//		}
//		if (socketWriter.getUpstreamStatus() >= AbstractStreamProducer.CLOSED) {
//			try {
//				channel.shutdownOutput();
//			} catch (IOException e) {
//				logger.error("shutdownOutput error {} for {}", e.toString(), this);
//			}
//		}
	}

	/**
	 * Sends received bytes to StreamConsumer
	 *
	 * @param buf received ByteBuffer
	 */
	@Override
	protected void onRead(ByteBuf buf) {
		assert eventloop.inEventloopThread();
		try {
			socketReader.send(buf);
			onRead();
		} catch (Exception e) {
			onInternalException(e);
		}
	}

	@Override
	protected void onRead() {
	}

	@Override
	protected void onWriteFlushed() {
		if (socketWriter.getStatus() == AbstractStreamConsumer.END_OF_STREAM) {
//			socketWriter.closeUpstream();
			closeIfDone();
		} else {
			socketWriter.resume();
		}
	}

	@Override
	protected void onReadException(Exception e) {
		logger.warn("onReadException", e);
//		socketReader.onConsumerError(e);
		socketReader.closeWithError(e);
	}

	@Override
	protected void onWriteException(Exception e) {
		logger.warn("onWriteException", e);
//		socketWriter.closeUpstreamWithError(e);
		closeIfDone();
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return name != null ? name : super.toString();
	}
}
