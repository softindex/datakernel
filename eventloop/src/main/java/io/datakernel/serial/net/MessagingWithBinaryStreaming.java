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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.*;
import io.datakernel.util.Taggable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.serial.ByteBufsSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;

/**
 * Represent the TCP connection which  processes received items with {@link SerialSupplier} and {@link SerialConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public final class MessagingWithBinaryStreaming<I, O> implements Messaging<I, O>, Taggable {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final AsyncTcpSocket socket;

	private final MessagingSerializer<I, O> serializer;
	private final ByteBufsParser<I> parser;

	private final ByteBufQueue bufs = new ByteBufQueue();
	private final ByteBufsSupplier bufsSupplier;
	private final SerialSupplier<ByteBuf> socketReader;
	private final SerialConsumer<ByteBuf> socketWriter;

	private Throwable closedException;

	private boolean readDone;
	private boolean writeDone;
	@Nullable
	private Object tag;

	// region creators
	private MessagingWithBinaryStreaming(AsyncTcpSocket socket, MessagingSerializer<I, O> serializer) {
		this.socket = socket;
		this.serializer = serializer;
		this.socketReader = socket.reader();
		this.socketWriter = socket.writer();
		this.bufsSupplier = ByteBufsSupplier.ofProvidedQueue(bufs,
				() -> this.socket.read()
						.thenCompose(buf -> {
							if (buf != null) {
								bufs.add(buf);
								return Stage.complete();
							} else {
								return Stage.ofException(UNEXPECTED_END_OF_STREAM_EXCEPTION);
							}
						})
						.whenException(this::closeWithError),
				Stage::complete,
				this);
		this.parser = bufs -> {
			ByteBuf buf = bufs.takeRemaining();
			I maybeResult = this.serializer.tryDeserialize(buf);
			if (buf.canRead()) {
				bufs.add(buf);
			} else {
				buf.recycle();
			}
			return maybeResult;
		};
	}

	public static <I, O> MessagingWithBinaryStreaming<I, O> create(AsyncTcpSocket socket,
			@Nullable MessagingSerializer<I, O> serializer) {
		MessagingWithBinaryStreaming<I, O> messaging = new MessagingWithBinaryStreaming<>(socket, serializer);
		messaging.prefetch();
		return messaging;
	}
	// endregion

	private void prefetch() {
		if (bufs.isEmpty()) {
			socket.read()
					.whenResult(buf -> {
						if (buf != null) {
							bufs.add(buf);
						} else {
							readDone = true;
							closeIfDone();
						}
					})
					.whenException(this::closeWithError);
		}
	}

	@Override
	public Stage<I> receive() {
		return bufsSupplier.parse(parser)
				.thenRun(this::prefetch)
				.whenException(this::closeWithError);
	}

	@Override
	public Stage<Void> send(O msg) {
		return socket.write(serializer.serialize(msg));
	}

	@Override
	public Stage<Void> sendEndOfStream() {
		return socket.write(null)
				.thenRun(() -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(this::closeWithError);
	}

	@Override
	public SerialConsumer<ByteBuf> sendBinaryStream() {
		return socketWriter
				.withAcknowledgement(stage -> stage
						.thenRun(() -> {
							writeDone = true;
							closeIfDone();
						})
						.whenException(this::closeWithError));
	}

	@Override
	public SerialSupplier<ByteBuf> receiveBinaryStream() {
		return SerialSuppliers.concat(SerialSupplier.ofIterator(bufs.asIterator()), socketReader)
				.withEndOfStream(stage -> stage
						.thenRun(() -> {
							readDone = true;
							closeIfDone();
						})
						.whenException(this::closeWithError));
	}

	@Override
	public void closeWithError(Throwable e) {
		if (isClosed()) return;
		closedException = e;
		socket.close();
		bufs.recycle();
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			close();
		}
	}

	public boolean isClosed() {
		return closedException != null;
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
