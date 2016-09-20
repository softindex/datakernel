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

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Represent the TCP connection which  processes received items with {@link StreamProducer} and {@link StreamConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public final class MessagingWithBinaryStreaming<I, O> implements AsyncTcpSocket.EventHandler, Messaging<I, O> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final AsyncTcpSocket asyncTcpSocket;
	private final MessagingSerializer<I, O> serializer;

	private ByteBuf readBuf;
	private boolean readEndOfStream;
	private ReceiveMessageCallback<I> receiveMessageCallback;
	private List<CompletionCallback> writeCallbacks = new ArrayList<>();
	private boolean writeEndOfStreamRequest;
	private SocketStreamProducer socketReader;
	private SocketStreamConsumer socketWriter;

	private Exception closedException;

	private boolean readDone;
	private boolean writeDone;

	// region creators
	private MessagingWithBinaryStreaming(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, MessagingSerializer<I, O> serializer) {
		this.eventloop = eventloop;
		this.asyncTcpSocket = asyncTcpSocket;
		this.serializer = serializer;
	}

	public static <I, O> MessagingWithBinaryStreaming<I, O> create(Eventloop eventloop,
	                                                               AsyncTcpSocket asyncTcpSocket,
	                                                               MessagingSerializer<I, O> serializer) {
		return new MessagingWithBinaryStreaming<>(eventloop, asyncTcpSocket, serializer);
	}
	// endregion

	@Override
	public void receive(ReceiveMessageCallback<I> callback) {
		checkState(socketReader == null && receiveMessageCallback == null);

		if (closedException != null) {
			callback.onException(closedException);
			return;
		}

		receiveMessageCallback = callback;
		if (readBuf != null || readEndOfStream) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (socketReader == null && receiveMessageCallback != null) {
						tryReadMessage();
					}
				}
			});
		} else {
			asyncTcpSocket.read();
		}
	}

	private void tryReadMessage() {
		if (readBuf != null && receiveMessageCallback != null) {
			try {
				I message = serializer.tryDeserialize(readBuf);
				if (message == null) {
					asyncTcpSocket.read();
				} else {
					if (!readBuf.canRead()) {
						readBuf.recycle();
						readBuf = null;
						if (!readEndOfStream) {
							asyncTcpSocket.read();
						}
					}
					takeReadCallback().onReceive(message);
				}
			} catch (ParseException e) {
				takeReadCallback().onException(e);
			}
		}
		if (readBuf == null && readEndOfStream) {
			if (receiveMessageCallback != null) {
				takeReadCallback().onReceiveEndOfStream();
			}
		}
	}

	private ReceiveMessageCallback<I> takeReadCallback() {
		ReceiveMessageCallback<I> callback = this.receiveMessageCallback;
		receiveMessageCallback = null;
		return callback;
	}

	@Override
	public void send(O msg, CompletionCallback callback) {
		checkState(socketWriter == null && !writeEndOfStreamRequest);

		if (closedException != null) {
			callback.onException(closedException);
			return;
		}

		writeCallbacks.add(callback);
		ByteBuf buf = serializer.serialize(msg);
		asyncTcpSocket.write(buf);
	}

	@Override
	public void sendEndOfStream(CompletionCallback callback) {
		checkState(socketWriter == null && !writeEndOfStreamRequest);

		if (closedException != null) {
			callback.onException(closedException);
			return;
		}

		writeEndOfStreamRequest = true;
		writeCallbacks.add(callback);
		asyncTcpSocket.writeEndOfStream();
	}

	public void sendBinaryStreamFrom(StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		checkState(socketWriter == null && !writeEndOfStreamRequest);

		writeCallbacks.clear();
		if (closedException != null) {
			callback.onException(closedException);
			return;
		}

		socketWriter = SocketStreamConsumer.create(eventloop, asyncTcpSocket, callback);
		producer.streamTo(socketWriter);
	}

	public void receiveBinaryStreamTo(StreamConsumer<ByteBuf> consumer, final CompletionCallback callback) {
		checkState(this.socketReader == null && this.receiveMessageCallback == null);

		if (closedException != null) {
			callback.onException(closedException);
			return;
		}

		socketReader = SocketStreamProducer.create(eventloop, asyncTcpSocket, callback);
		socketReader.streamTo(consumer);
		if (readBuf != null || readEndOfStream) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (readBuf != null) {
						readUnconsumedBuf();
					}
					if (readEndOfStream) {
						socketReader.onReadEndOfStream();
					}
				}
			});
		}
	}

	@Override
	public void close() {
		asyncTcpSocket.close();
		if (readBuf != null) {
			readBuf.recycle();
			readBuf = null;
		}
	}

	/**
	 * Is called after connection registration. Wires socketReader with StreamConsumer specified by,
	 * and socketWriter with StreamProducer, that are specified by overridden method {@code wire} of subclass.
	 * If StreamConsumer is null, items from socketReader are ignored. If StreamProducer is null, socketWriter
	 * gets EndOfStream signal.
	 */
	@Override
	public void onRegistered() {
		asyncTcpSocket.read();
	}

	private void readUnconsumedBuf() {
		assert readBuf != null;
		socketReader.onRead(readBuf);
		readBuf = null;
	}

	@Override
	public void onRead(ByteBuf buf) {
		logger.trace("onRead", this);
		assert eventloop.inEventloopThread();
		if (socketReader == null) {
			if (readBuf == null) {
				readBuf = ByteBufPool.allocate(Math.max(8192, buf.tailRemaining()));
			}
			readBuf = ByteBufPool.append(readBuf, buf);
			tryReadMessage();
		} else {
			if (readBuf != null) {
				readUnconsumedBuf();
			}
			socketReader.onRead(buf);
		}
	}

	@Override
	public void onReadEndOfStream() {
		logger.trace("onShutdownInput", this);
		readEndOfStream = true;
		if (socketReader == null) {
			tryReadMessage();
		} else {
			if (readBuf != null) {
				readUnconsumedBuf();
			}
			socketReader.onReadEndOfStream();
		}
		readDone = true;
		closeIfDone();
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			asyncTcpSocket.close();
		}
	}

	@Override
	public void onWrite() {
		logger.trace("onWrite", this);
		if (socketWriter == null) {
			List<CompletionCallback> callbacks = this.writeCallbacks;
			writeCallbacks = new ArrayList<>();
			for (CompletionCallback callback : callbacks) {
				callback.onComplete();
			}
			if (writeEndOfStreamRequest)
				writeDone = true;
		} else {
			socketWriter.onWrite();
			if (socketWriter.getConsumerStatus().isClosed())
				writeDone = true;
		}
		closeIfDone();
	}

	@Override
	public void onClosedWithError(Exception e) {
		logger.trace("onClosedWithError", this);

		if (socketReader != null) {
			socketReader.closeWithError(e);
		} else if (socketWriter != null) {
			socketWriter.closeWithError(e);
		} else {
			closedException = e;
		}

		if (receiveMessageCallback != null) {
			receiveMessageCallback.onException(e);
		} else if (!writeCallbacks.isEmpty()) {
			for (CompletionCallback writeCallback : writeCallbacks) {
				writeCallback.onException(e);
			}
		}

		if (readBuf != null) {
			readBuf.recycle();
			readBuf = null;
		}
	}

	@Override
	public String toString() {
		return "{asyncTcpSocket=" + asyncTcpSocket + "}";
	}

}
