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

package io.datakernel.remotefs;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.ByteBufSerializer;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.net.SocketSettings;
import io.datakernel.remotefs.RemoteFsCommands.*;
import io.datakernel.remotefs.RemoteFsResponses.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.csp.binary.ByteBufSerializer.ofJsonCodec;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link FsClient} which connects to a single {@link RemoteFsServer} and communicates with it.
 */
public final class RemoteFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsClient.class);

	public static final RemoteFsException UNEXPECTED_END_OF_STREAM = new RemoteFsException(RemoteFsClient.class, "Unexpected end of stream");
	private static final ByteBufSerializer<FsResponse, FsCommand> SERIALIZER =
			ofJsonCodec(RemoteFsResponses.CODEC, RemoteFsCommands.CODEC);

	private final Eventloop eventloop;
	private final InetSocketAddress address;

	private SocketSettings socketSettings = SocketSettings.create();

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

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		checkNotNull(filename, "fileName");

		return connect(address)
				.thenCompose(messaging ->
						messaging.send(new Upload(filename, offset))
								.thenApply($ -> messaging.sendBinaryStream()
										.withAcknowledgement(ack -> ack
												.thenCompose($2 -> messaging.receive())
												.thenCompose(msg -> {
													messaging.close();
													if (msg instanceof UploadFinished) {
														return Promise.complete();
													}
													if (msg instanceof ServerError) {
														return Promise.ofException(new RemoteFsException(RemoteFsClient.class, ((ServerError) msg).getMessage()));
													}
													if (msg != null) {
														return Promise.ofException(new RemoteFsException(RemoteFsClient.class, "Invalid message received: " + msg));
													}
													return Promise.ofException(new RemoteFsException(RemoteFsClient.class, "Unexpected end of stream for: " + filename));
												})
												.whenException(e -> {
													messaging.close(e);
													logger.warn("Cancelled while trying to upload file " + filename + " (" + e + "): " + this);
												})
												.whenComplete(uploadFinishPromise.recordStats())))
								.whenException(e -> {
									messaging.close(e);
									logger.warn("Error while trying to upload file " + filename + " (" + e + "): " + this);
								}))
				.whenComplete(toLogger(logger, TRACE, "upload", filename, this))
				.whenComplete(uploadStartPromise.recordStats());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		checkNotNull(filename, "fileName");

		return connect(address)
				.thenCompose(messaging ->
						messaging.send(new Download(filename, offset, length))
								.thenCompose($ -> messaging.receive())
								.thenCompose(msg -> {
									if (msg instanceof DownloadSize) {
										long receivingSize = ((DownloadSize) msg).getSize();

										logger.trace("download size for file {} is {}: {}", filename, receivingSize, this);

										long[] size = {0};
										return Promise.of(messaging.receiveBinaryStream()
												.peek(buf -> size[0] += buf.readRemaining())
												.withEndOfStream(eos -> eos
														.thenCompose($ -> messaging.sendEndOfStream())
														.thenCompose(result -> size[0] == receivingSize ?
																Promise.of(result) :
																Promise.ofException(new RemoteFsException(RemoteFsClient.class,
																		"Invalid stream size for file " + filename +
																				" (offset " + offset + ", length " + length + ")," +
																				" expected: " + receivingSize +
																				" actual: " + size[0])))
														.whenComplete(downloadFinishPromise.recordStats())
														.whenResult($1 -> messaging.close())));
									}
									if (msg instanceof ServerError) {
										return Promise.ofException(new RemoteFsException(RemoteFsClient.class, ((ServerError) msg).getMessage()));
									}
									if (msg != null) {
										return Promise.ofException(new RemoteFsException(RemoteFsClient.class, "Invalid message received: " + msg));
									}
									logger.warn(this + ": Received unexpected end of stream");
									return Promise.ofException(new RemoteFsException(RemoteFsClient.class, "Unexpected end of stream for: " + filename));
								})
								.whenException(e -> {
									messaging.close(e);
									logger.warn("Error trying to download file " + filename + " (offset=" + offset + ", length=" + length + ") (" + e + "): " + this);
								}))
				.whenComplete(toLogger(logger, TRACE, "download", filename, offset, length, this))
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		checkNotNull(changes, "changes");

		return simpleCommand(new Move(changes), MoveFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, TRACE, "move", changes, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		checkNotNull(changes, "changes");

		return simpleCommand(new Copy(changes), CopyFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, TRACE, "copy", changes, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		checkNotNull(glob, "glob");

		return simpleCommand(new Delete(glob), DeleteFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, TRACE, "delete", glob, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		checkNotNull(glob, "glob");

		return simpleCommand(new RemoteFsCommands.List(glob), ListFinished.class, ListFinished::getFiles)
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	private <T, R extends FsResponse> Promise<T> simpleCommand(FsCommand command, Class<R> responseType, Function<R, T> answerExtractor) {
		return connect(address)
				.thenCompose(messaging ->
						messaging.send(command)
								.thenCompose($ -> messaging.receive())
								.thenCompose(msg -> {
									messaging.close();
									if (msg == null) {
										return Promise.ofException(UNEXPECTED_END_OF_STREAM);
									}
									if (msg.getClass() == responseType) {
										return Promise.of(answerExtractor.apply(responseType.cast(msg)));
									}
									if (msg instanceof ServerError) {
										return Promise.ofException(new RemoteFsException(RemoteFsClient.class, ((ServerError) msg).getMessage()));
									}
									return Promise.ofException(new RemoteFsException(RemoteFsClient.class, "Invalid message received: " + msg));
								})
								.whenException(e -> {
									messaging.close(e);
									logger.warn("Error while processing command " + command + " (" + e + ") : " + this);
								}));
	}

	private Promise<MessagingWithBinaryStreaming<FsResponse, FsCommand>> connect(InetSocketAddress address) {
		return AsyncTcpSocketImpl.connect(address, 0, socketSettings)
				.thenApply(socket -> MessagingWithBinaryStreaming.create(socket, SERIALIZER))
				.whenResult($ -> logger.trace("connected to [{}]: {}", address, this))
				.whenException(e -> logger.warn("failed connecting to [" + address + "] (" + e + "): " + this))
				.whenComplete(connectPromise.recordStats());
	}

	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

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
