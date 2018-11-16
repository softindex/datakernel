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

package io.datakernel.csp.net;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufSerializer;
import io.datakernel.eventloop.AsyncTcpSocket;

import static io.datakernel.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;

/**
 * Represent the TCP connection which  processes received items with {@link ChannelSupplier} and {@link ChannelConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public final class MessagingWithBinaryStreaming<I, O> implements Messaging<I, O> {
	private final AsyncTcpSocket socket;

	private final ByteBufSerializer<I, O> serializer;

	private final ByteBufQueue bufs = new ByteBufQueue();
	private final BinaryChannelSupplier bufsSupplier;

	private Throwable closedException;

	private boolean readDone;
	private boolean writeDone;

	// region creators
	private MessagingWithBinaryStreaming(AsyncTcpSocket socket, ByteBufSerializer<I, O> serializer) {
		this.socket = socket;
		this.serializer = serializer;
		this.bufsSupplier = BinaryChannelSupplier.ofProvidedQueue(bufs,
				() -> this.socket.read()
						.thenCompose(buf -> {
							if (buf != null) {
								bufs.add(buf);
								return Promise.complete();
							} else {
								return Promise.ofException(UNEXPECTED_END_OF_STREAM_EXCEPTION);
							}
						})
						.whenException(this::close),
				Promise::complete,
				this);
	}

	public static <I, O> MessagingWithBinaryStreaming<I, O> create(AsyncTcpSocket socket,
			ByteBufSerializer<I, O> serializer) {
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
					.whenException(this::close);
		}
	}

	@Override
	public Promise<I> receive() {
		return bufsSupplier.parse(serializer)
				.whenResult($ -> prefetch())
				.whenException(this::close);
	}

	@Override
	public Promise<Void> send(O msg) {
		return socket.write(serializer.serialize(msg));
	}

	@Override
	public Promise<Void> sendEndOfStream() {
		return socket.write(null)
				.whenResult($ -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(this::close);
	}

	@Override
	public ChannelConsumer<ByteBuf> sendBinaryStream() {
		return ChannelConsumer.ofSocket(socket)
				.withAcknowledgement(ack -> ack
						.whenResult($ -> {
							writeDone = true;
							closeIfDone();
						}));
	}

	@Override
	public ChannelSupplier<ByteBuf> receiveBinaryStream() {
		return ChannelSuppliers.concat(ChannelSupplier.ofIterator(bufs.asIterator()), ChannelSupplier.ofSocket(socket))
				.withEndOfStream(eos -> eos
						.whenResult($ -> {
							readDone = true;
							closeIfDone();
						}));
	}

	@Override
	public void close(Throwable e) {
		if (isClosed()) return;
		closedException = e;
		socket.close(e);
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
	public String toString() {
		return "MessagingWithBinaryStreaming{socket=" + socket + "}";
	}
}
