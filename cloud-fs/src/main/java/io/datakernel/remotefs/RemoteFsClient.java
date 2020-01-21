/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.remotefs;

import io.datakernel.async.service.EventloopService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.common.ref.RefLong;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelConsumers;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.ByteBufSerializer;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.net.AsyncTcpSocketImpl;
import io.datakernel.promise.Promise;
import io.datakernel.promise.jmx.PromiseStats;
import io.datakernel.remotefs.RemoteFsCommands.*;
import io.datakernel.remotefs.RemoteFsResponses.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.csp.binary.ByteBufSerializer.ofJsonCodec;
import static io.datakernel.remotefs.RemoteFsUtils.KNOWN_ERRORS;

/**
 * An implementation of {@link FsClient} which connects to a single {@link RemoteFsServer} and communicates with it.
 */
public final class RemoteFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsClient.class);

	public static final StacklessException INVALID_MESSAGE = new StacklessException(RemoteFsClient.class, "Invalid or unexpected message received");
	public static final StacklessException TOO_MUCH_DATA = new StacklessException(RemoteFsClient.class, "Received more bytes than expected");
	public static final StacklessException UNEXPECTED_END_OF_STREAM = new StacklessException(RemoteFsClient.class, "Unexpected end of stream");
	public static final StacklessException UNKNOWN_SERVER_ERROR = new StacklessException(RemoteFsClient.class, "Unknown server error occured");

	private static final ByteBufSerializer<FsResponse, FsCommand> SERIALIZER =
			ofJsonCodec(RemoteFsResponses.CODEC, RemoteFsCommands.CODEC);

	private final Eventloop eventloop;
	private final InetSocketAddress address;

	private SocketSettings socketSettings = SocketSettings.createDefault();

	//region JMX
	private final PromiseStats connectPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	//endregion

	// region creators
	private RemoteFsClient(Eventloop eventloop, InetSocketAddress address) {
		this.eventloop = eventloop;
		this.address = address;
	}

	public static RemoteFsClient create(Eventloop eventloop, InetSocketAddress address) {
		return new RemoteFsClient(eventloop, address);
	}

	public RemoteFsClient withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}
	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String filename, long offset, long revision) {
		return connect(address)
				.then(messaging ->
						messaging.send(new Upload(filename, offset, revision))
								.then($ -> messaging.receive())
								.then(msg -> {
									if (!(msg instanceof UploadAck)) {
										return handleInvalidResponse(msg);
									}
									if (!((UploadAck) msg).isOk()) {
										return Promise.of(ChannelConsumers.<ByteBuf>recycling());
									}
									return Promise.of(messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then($2 -> messaging.receive())
													.then(msg2 -> {
														messaging.close();
														return msg2 instanceof UploadFinished ?
																Promise.complete() :
																handleInvalidResponse(msg2);
													})
													.whenException(e -> {
														messaging.close(e);
														logger.warn("Cancelled while trying to upload file " + filename + " (" + e + "): " + this);
													})
													.whenComplete(uploadFinishPromise.recordStats())));
								})
								.whenException(e -> {
									messaging.close(e);
									logger.warn("Error while trying to upload file " + filename + " (" + e + "): " + this);
								}))
				.whenComplete(toLogger(logger, "upload", filename, this))
				.whenComplete(uploadStartPromise.recordStats());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		return connect(address)
				.then(messaging ->
						messaging.send(new Download(name, offset, length))
								.then($ -> messaging.receive())
								.then(msg -> {
									if (!(msg instanceof DownloadSize)) {
										return handleInvalidResponse(msg);
									}
									long receivingSize = ((DownloadSize) msg).getSize();

									logger.trace("download size for file {} is {}: {}", name, receivingSize, this);

									RefLong size = new RefLong(0);
									return Promise.of(messaging.receiveBinaryStream()
											.peek(buf -> size.inc(buf.readRemaining()))
											.withEndOfStream(eos -> eos
													.then($ -> messaging.sendEndOfStream())
													.then(result -> {
														if (size.get() == receivingSize) {
															return Promise.of(result);
														}
														logger.error("invalid stream size for file " + name +
																" (offset " + offset + ", length " + length + ")," +
																" expected: " + receivingSize +
																" actual: " + size.get());
														return Promise.ofException(size.get() < receivingSize ? UNEXPECTED_END_OF_STREAM : TOO_MUCH_DATA);
													})
													.whenComplete(downloadFinishPromise.recordStats())
													.whenResult($1 -> messaging.close())));
								})
								.whenException(e -> {
									messaging.close(e);
									logger.warn("error trying to download file " + name + " (offset=" + offset + ", length=" + length + ") (" + e + "): " + this);
								}))
				.whenComplete(toLogger(logger, "download", name, offset, length, this))
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target, long targetRevision, long tombstoneRevision) {
		return simpleCommand(new Move(name, target, targetRevision, tombstoneRevision), MoveFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "move", name, target, targetRevision, tombstoneRevision, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target, long targetRevision) {
		return simpleCommand(new Copy(name, target, targetRevision), CopyFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "copy", name, target, targetRevision, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name, long revision) {
		return simpleCommand(new Delete(name, revision), DeleteFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "delete", name, revision, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(@NotNull String glob) {
		return simpleCommand(new RemoteFsCommands.List(glob, true), ListFinished.class, ListFinished::getFiles)
				.whenComplete(toLogger(logger, "listEntities", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return simpleCommand(new RemoteFsCommands.List(glob, false), ListFinished.class, ListFinished::getFiles)
				.whenComplete(toLogger(logger, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	private Promise<MessagingWithBinaryStreaming<FsResponse, FsCommand>> connect(InetSocketAddress address) {
		return AsyncTcpSocketImpl.connect(address, 0, socketSettings)
				.map(socket -> MessagingWithBinaryStreaming.create(socket, SERIALIZER))
				.whenResult($ -> logger.trace("connected to [{}]: {}", address, this))
				.whenException(e -> logger.warn("failed connecting to [" + address + "] (" + e + "): " + this))
				.whenComplete(connectPromise.recordStats());
	}

	private <T> Promise<T> handleInvalidResponse(@Nullable FsResponse msg) {
		if (msg == null) {
			logger.warn(this + ": Received unexpected end of stream");
			return Promise.ofException(UNEXPECTED_END_OF_STREAM);
		}
		if (msg instanceof ServerError) {
			int code = ((ServerError) msg).getCode();
			Throwable error = code >= 1 && code <= KNOWN_ERRORS.length ? KNOWN_ERRORS[code - 1] : UNKNOWN_SERVER_ERROR;
			return Promise.ofException(error);
		}
		return Promise.ofException(INVALID_MESSAGE);
	}

	private <T, R extends FsResponse> Promise<T> simpleCommand(FsCommand command, Class<R> responseType, Function<R, T> answerExtractor) {
		return connect(address)
				.then(messaging ->
						messaging.send(command)
								.then($ -> messaging.receive())
								.then(msg -> {
									messaging.close();
									if (msg != null && msg.getClass() == responseType) {
										return Promise.of(answerExtractor.apply(responseType.cast(msg)));
									}
									return handleInvalidResponse(msg);
								})
								.whenException(e -> {
									messaging.close(e);
									logger.warn("Error while processing command " + command + " (" + e + ") : " + this);
								}));
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "RemoteFsClient{address=" + address + '}';
	}

	//region JMX
	@JmxAttribute
	public PromiseStats getConnectPromise() {
		return connectPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadStartPromise() {
		return uploadStartPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadFinishPromise() {
		return uploadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadStartPromise() {
		return downloadStartPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadFinishPromise() {
		return downloadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}
	//endregion
}
