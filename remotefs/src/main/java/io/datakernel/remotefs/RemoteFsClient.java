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

package io.datakernel.remotefs;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.net.SocketSettings;
import io.datakernel.remotefs.RemoteFsCommands.Download;
import io.datakernel.remotefs.RemoteFsCommands.FsCommand;
import io.datakernel.remotefs.RemoteFsCommands.Upload;
import io.datakernel.remotefs.RemoteFsResponses.*;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.net.MessagingSerializer;
import io.datakernel.serial.net.MessagingWithBinaryStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.serial.net.MessagingSerializers.ofJson;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link FsClient} which connects to a single {@link RemoteFsServer} and communicates with it.
 */
public final class RemoteFsClient implements FsClient, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsClient.class);

	private static final MessagingSerializer<FsResponse, FsCommand> SERIALIZER =
			ofJson(RemoteFsResponses.ADAPTER, RemoteFsCommands.ADAPTER);

	private final Eventloop eventloop;
	private final InetSocketAddress address;

	private SocketSettings socketSettings = SocketSettings.create();

	//region JMX
	private final StageStats connectStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats uploadStartStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats uploadFinishStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats downloadStartStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats downloadFinishStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats moveStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats copyStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats listStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats deleteStage = StageStats.create(Duration.ofMinutes(5));
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
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		checkNotNull(filename, "fileName");

		return connect(address)
				.thenCompose(messaging ->
						messaging.send(new Upload(filename, offset))
								.thenApply($ -> messaging.sendBinaryStream()
										.withAcknowledgement(acknowledgement -> acknowledgement
												.thenCompose($2 -> messaging.receive())
												.thenCompose(msg -> {
													messaging.close();
													if (msg instanceof UploadFinished) {
														return Stage.complete();
													}
													if (msg instanceof ServerError) {
														return Stage.ofException(new RemoteFsException(((ServerError) msg).getMessage()));
													}
													if (msg != null) {
														return Stage.ofException(new RemoteFsException("Invalid message received: " + msg));
													}
													return Stage.ofException(new RemoteFsException());
												})
												.whenException(e -> {
													messaging.close();
													logger.warn("Cancelled while trying to upload file " + filename + " (" + e + "): " + this);
												})
												.whenComplete(uploadFinishStage.recordStats())))
								.whenException(e -> {
									messaging.close();
									logger.warn("Error while trying to upload file " + filename + " (" + e + "): " + this);
								}))
				.whenComplete(toLogger(logger, TRACE, "upload", filename, this))
				.whenComplete(uploadStartStage.recordStats());
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
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
										return Stage.of(messaging.receiveBinaryStream()
												.peek(buf -> size[0] += buf.readRemaining())
												.withEndOfStream(endOfStream -> endOfStream
														.thenCompose($ -> messaging.sendEndOfStream())
														.thenException($ -> size[0] != receivingSize ?
																new IOException("Invalid stream size for file " + filename +
																		" (offset " + offset + ", length " + length +
																		"), expected: " + receivingSize +
																		" actual: " + size[0]) :
																null)
														.whenComplete(downloadFinishStage.recordStats())
														.thenRun(messaging::close)));
									}
									if (msg instanceof ServerError) {
										return Stage.ofException(new RemoteFsException(((ServerError) msg).getMessage()));
									}
									if (msg != null) {
										return Stage.ofException(new RemoteFsException("Invalid message received: " + msg));
									}
									logger.warn(this + ": Received unexpected end of stream");
									return Stage.ofException(new RemoteFsException("Unexpected end of stream for: " + filename));
								})
								.whenException(e -> {
									messaging.close();
									logger.warn("Error trying to download file " + filename + " (offset=" + offset + ", length=" + length + ") (" + e + "): " + this);
								}))
				.whenComplete(toLogger(logger, TRACE, "download", filename, offset, length, this))
				.whenComplete(downloadStartStage.recordStats());
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		checkNotNull(changes, "changes");

		return simpleCommand(new RemoteFsCommands.Move(changes), MoveFinished.class, MoveFinished::getMoved)
				.whenComplete(toLogger(logger, TRACE, "move", changes, this))
				.whenComplete(moveStage.recordStats());
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		checkNotNull(changes, "changes");

		return simpleCommand(new RemoteFsCommands.Copy(changes), CopyFinished.class, CopyFinished::getCopied)
				.whenComplete(toLogger(logger, TRACE, "copy", changes, this))
				.whenComplete(copyStage.recordStats());
	}

	@Override
	public Stage<Void> delete(String glob) {
		checkNotNull(glob, "glob");

		return simpleCommand(new RemoteFsCommands.Delete(glob), DeleteFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, TRACE, "delete", glob, this))
				.whenComplete(deleteStage.recordStats());
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		checkNotNull(glob, "glob");

		return simpleCommand(new RemoteFsCommands.List(glob), ListFinished.class, ListFinished::getFiles)
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listStage.recordStats());
	}

	private <T, R extends FsResponse> Stage<T> simpleCommand(FsCommand command, Class<R> responseType, Function<R, T> answerExtractor) {
		return connect(address)
				.thenCompose(messaging ->
						messaging.send(command)
								.thenCompose($ -> messaging.receive())
								.thenCompose(msg -> {
									messaging.close();
									if (msg == null) {
										return Stage.ofException(new RemoteFsException("Unexpected end of stream"));
									}
									if (msg.getClass() == responseType) {
										return Stage.of(answerExtractor.apply(responseType.cast(msg)));
									}
									if (msg instanceof ServerError) {
										return Stage.ofException(new RemoteFsException(((ServerError) msg).getMessage()));
									}
									return Stage.ofException(new RemoteFsException("Invalid message received: " + msg));
								})
								.whenException(e -> {
									messaging.close();
									logger.warn("Error while processing command " + command + " (" + e + ") : " + this);
								}));
	}

	private Stage<MessagingWithBinaryStreaming<FsResponse, FsCommand>> connect(InetSocketAddress address) {
		return eventloop.connect(address)
				.thenRun(() -> logger.trace("connected to [{}]: {}", address, this))
				.thenApply(channel -> AsyncTcpSocketImpl.wrapChannel(eventloop, channel, socketSettings))
				.thenApply(socket -> MessagingWithBinaryStreaming.create(socket, SERIALIZER))
				.whenException(e -> logger.warn("failed connecting to [" + address + "] (" + e + "): " + this))
				.whenComplete(connectStage.recordStats());
	}

	@Override
	public Stage<Void> start() {
		return Stage.complete();
	}

	@Override
	public Stage<Void> stop() {
		return Stage.complete();
	}

	@Override
	public String toString() {
		return "RemoteFsClient{address=" + address + '}';
	}

	//region JMX
	@JmxAttribute
	public StageStats getConnectStage() {
		return connectStage;
	}

	@JmxAttribute
	public StageStats getUploadStartStage() {
		return uploadStartStage;
	}

	@JmxAttribute
	public StageStats getUploadFinishStage() {
		return uploadFinishStage;
	}

	@JmxAttribute
	public StageStats getDownloadStartStage() {
		return downloadStartStage;
	}

	@JmxAttribute
	public StageStats getDownloadFinishStage() {
		return downloadFinishStage;
	}

	@JmxAttribute
	public StageStats getMoveStage() {
		return moveStage;
	}

	@JmxAttribute
	public StageStats getCopyStage() {
		return copyStage;
	}

	@JmxAttribute
	public StageStats getListStage() {
		return listStage;
	}

	@JmxAttribute
	public StageStats getDeleteStage() {
		return deleteStage;
	}
	//endregion
}
