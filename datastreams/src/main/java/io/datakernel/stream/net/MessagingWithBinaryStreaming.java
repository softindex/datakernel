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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Callback;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.util.Taggable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

/**
 * Represent the TCP connection which  processes received items with {@link StreamProducer} and {@link StreamConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public final class MessagingWithBinaryStreaming<I, O> implements AsyncTcpSocket.EventHandler, Messaging<I, O>, Taggable {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final AsyncTcpSocket socket;
	private final MessagingSerializer<I, O> serializer;

	@Nullable
	private ByteBuf readBuf;

	@Nullable
	private Callback<I> receiveMessageCallback;

	private boolean readEndOfStream;
	private List<SettableStage<Void>> writeCallbacks = new ArrayList<>();
	private boolean writeEndOfStreamRequest;
	private SocketStreamProducer socketReader;
	private SocketStreamConsumer socketWriter;

	private Exception closedException;

	private boolean readDone;
	private boolean writeDone;

	@Nullable
	private Object tag;

	// region creators
	private MessagingWithBinaryStreaming(AsyncTcpSocket socket, MessagingSerializer<I, O> serializer) {
		this.socket = socket;
		this.serializer = serializer;
	}

	public static <I, O> MessagingWithBinaryStreaming<I, O> create(AsyncTcpSocket asyncTcpSocket,
																   MessagingSerializer<I, O> serializer) {
		return new MessagingWithBinaryStreaming<>(asyncTcpSocket, serializer);
	}
	// endregion

	@Override
	public Stage<I> receive() {
		checkState(socketReader == null, "Cannot try to receive a message while receiving raw binary data");
		checkState(receiveMessageCallback == null, "Cannot try to receive a message while already trying to receive a message");

		if (closedException != null) {
			return Stage.ofException(closedException);
		}

		SettableStage<I> result = SettableStage.create();
		this.receiveMessageCallback = result;
		if (readBuf != null || readEndOfStream) {
			eventloop.post(() -> {
				if (socketReader == null && this.receiveMessageCallback != null) {
					tryReadMessage();
				}
			});
		} else {
			socket.read();
		}
		return result;
	}

	private void tryReadMessage() {
		if (readBuf != null && receiveMessageCallback != null) {
			try {
				I message = serializer.tryDeserialize(readBuf);
				if (message == null) {
					socket.read();
				} else {
					if (!readBuf.canRead()) {
						readBuf.recycle();
						readBuf = null;
						if (!readEndOfStream) {
							socket.read();
						}
					}
					if (logger.isTraceEnabled()) {
						logger.trace("received message {}: {}", message, this);
					}
					takeReadCallback().set(message);
				}
			} catch (ParseException e) {
				logger.warn("error trying to deserialize a message: " + this, e);
				takeReadCallback().setException(e);
			}
		}
		if (readBuf == null && readEndOfStream) {
			if (receiveMessageCallback != null) {
				logger.warn("end of stream reached while trying to read a message: {}", this);
				takeReadCallback().set(null);
			}
		}
	}

	private Callback<I> takeReadCallback() {
		Callback<I> callback = this.receiveMessageCallback;
		receiveMessageCallback = null;
		assert callback != null;
		return callback;
	}

	@Override
	public Stage<Void> send(O msg) {
		checkState(socketWriter == null, "Cannot send messages while sending raw binary data");
		checkState(!writeEndOfStreamRequest, "Cannot send messages after end of stream was sent");

		if (closedException != null) {
			logger.warn("failed to send message " + msg + ": " + this, closedException);
			return Stage.ofException(closedException);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("sending message {}: {}", msg, this);
		}

		SettableStage<Void> stage = SettableStage.create();
		writeCallbacks.add(stage);
		ByteBuf buf = serializer.serialize(msg);
		socket.write(buf);

		return stage;
	}

	@Override
	public Stage<Void> sendEndOfStream() {
		checkState(socketWriter == null, "Cannot send end of stream while sending raw binary data");
		checkState(!writeEndOfStreamRequest, "Cannot send end of stream after end of stream was already sent");

		if (closedException != null) {
			logger.warn("failed to send end of stream: " + this, closedException);
			return Stage.ofException(closedException);
		}

		logger.trace("sending end of stream: {}", this);

		SettableStage<Void> stage = SettableStage.create();
		writeEndOfStreamRequest = true;
		socket.writeEndOfStream();

		if (writeCallbacks.isEmpty()) { // all writes are already done
			writeDone = true;
			closeIfDone();
			return Stage.of(null);
		}
		writeCallbacks.add(stage);
		return stage;
	}

	@Override
	public StreamConsumerWithResult<ByteBuf, Void> sendBinaryStream() {
		checkState(socketWriter == null, "Cannot send raw binary data while already sending raw binary data");
		checkState(!writeEndOfStreamRequest, "Cannot send raw binary data after end of stream was sent");

		writeCallbacks.clear();
		if (closedException != null) {
			logger.warn("failed to send binary data: " + this, closedException);
			return StreamConsumer.<ByteBuf>closingWithError(closedException).withEndOfStreamAsResult();
		}

		logger.trace("sending binary data: {}", this);

		socketWriter = SocketStreamConsumer.create(socket);
		return socketWriter.withResult(socketWriter.getSentStage());
	}

	@Override
	public StreamProducerWithResult<ByteBuf, Void> receiveBinaryStream() {
		checkState(socketReader == null, "Cannot receive raw binary data while already receiving raw binary data");
		checkState(receiveMessageCallback == null, "Cannot receive raw binary data while trying to receive a message");

		if (closedException != null) {
			logger.warn("failed to receive binary data: " + this, closedException);
			return StreamProducer.<ByteBuf>closingWithError(closedException).withEndOfStreamAsResult();
		}

		logger.trace("receiving binary data: {}", this);

		socketReader = SocketStreamProducer.create(socket);
		if (readBuf != null || readEndOfStream) {
			eventloop.post(() -> {
				if (readBuf != null) {
					readUnconsumedBuf();
				}
				if (readEndOfStream) {
					socketReader.onReadEndOfStream();
				}
			});
		}
		return socketReader.withEndOfStreamAsResult();
	}

	@Override
	public void close() {
		logger.info("closing: {}", this);
		socket.close();
		if (readBuf != null) {
			readBuf.recycle();
			readBuf = null;
		}
	}

	@Override
	public void onRegistered() {
		socket.read();
	}

	private void readUnconsumedBuf() {
		assert readBuf != null;
		socketReader.onRead(readBuf);
		readBuf = null;
	}

	@Override
	public void onRead(ByteBuf buf) {
		assert eventloop.inEventloopThread();
		if (socketReader == null) {
			if (readBuf == null) {
				readBuf = ByteBufPool.allocate(Math.max(8192, buf.writeRemaining()));
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
			socket.close();
		}
	}

	@Override
	public void onWrite() {
		if (socketWriter == null) {
			List<SettableStage<Void>> callbacks = this.writeCallbacks;
			writeCallbacks = new ArrayList<>();
			for (SettableStage<Void> callback : callbacks) {
				callback.set(null);
			}
			if (writeEndOfStreamRequest)
				writeDone = true;
		} else {
			socketWriter.onWrite();
			if (socketWriter.getStatus().isClosed())
				writeDone = true;
		}
		closeIfDone();
	}

	@Override
	public void onClosedWithError(Exception e) {
		logger.warn("closing with error: " + this, e);
		if (socketReader != null) {
			socketReader.closeWithError(e);
		} else if (socketWriter != null) {
			socketWriter.closeWithError(e);
		} else {
			closedException = e;
		}

		if (receiveMessageCallback != null) {
			receiveMessageCallback.setException(e);
		} else if (!writeCallbacks.isEmpty()) {
			for (SettableStage writeCallback : writeCallbacks) {
				writeCallback.setException(e);
			}
		}

		if (readBuf != null) {
			readBuf.recycle();
			readBuf = null;
		}
	}

	@Override
	public void setTag(@Nullable Object tag) {
		this.tag = tag;
	}

	@Nullable
	@Override
	public Object getTag() {
		return tag;
	}

	@Override
	public String toString() {
		return "Messaging" + (tag != null ? '(' + tag.toString() + ')' : "{socket=" + socket + '}');
	}
}
