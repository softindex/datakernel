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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.remotefs.RemoteFsCommands.*;
import io.datakernel.remotefs.RemoteFsResponses.*;
import io.datakernel.serial.net.Messaging;
import io.datakernel.serial.net.MessagingSerializer;
import io.datakernel.serial.net.MessagingSerializers;
import io.datakernel.serial.net.MessagingWithBinaryStreaming;

import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An implementation of {@link AbstractServer} for RemoteFs.
 * It exposes some given {@link FsClient} to the Internet in pair with {@link RemoteFsClient}
 */
public final class RemoteFsServer extends AbstractServer<RemoteFsServer> {
	private static final MessagingSerializer<FsCommand, FsResponse> SERIALIZER =
			MessagingSerializers.ofJson(RemoteFsCommands.ADAPTER, RemoteFsResponses.ADAPTER);

	private final Map<Class, MessagingHandler<FsCommand>> handlers = new HashMap<>();
	private final FsClient client;

	// region JMX
	private final StageStats handleRequestStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats uploadStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats downloadStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats moveStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats copyStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats listStage = StageStats.create(Duration.ofMinutes(5));
	private final StageStats deleteStage = StageStats.create(Duration.ofMinutes(5));
	// endregion

	private RemoteFsServer(Eventloop eventloop, FsClient client) {
		super(eventloop);
		this.client = client;
		addHandlers();
	}

	public static RemoteFsServer create(Eventloop eventloop, ExecutorService executor, Path storage) {
		return new RemoteFsServer(eventloop, LocalFsClient.create(eventloop, executor, storage));
	}

	public static RemoteFsServer create(Eventloop eventloop, FsClient client) {
		return new RemoteFsServer(eventloop, client);
	}

	public FsClient getClient() {
		return client;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<FsCommand, FsResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, SERIALIZER);
		messaging.receive()
				.thenCompose(msg -> {
					if (msg == null) {
						logger.warn("unexpected end of stream: {}", this);
						messaging.close();
						return Stage.complete();
					}
					MessagingHandler<FsCommand> handler = handlers.get(msg.getClass());
					if (handler == null) {
						return Stage.ofException(new Exception("no handler for " + msg + " " + this));
					}
					return handler.onMessage(messaging, msg);
				})
				.whenComplete(handleRequestStage.recordStats())
				.thenComposeEx(($, err) -> {
					if (err == null) {
						return Stage.complete();
					}
					logger.warn("got an error while handling message (" + err + ") : " + this);
					String prefix = err.getClass() != RemoteFsException.class ? err.getClass().getSimpleName() + ": " : "";
					return messaging.send(new ServerError(prefix + err.getMessage()))
							.thenCompose($2 -> messaging.sendEndOfStream())
							.thenRun(messaging::close);
				});
	}

	private void addHandlers() {
		onMessage(Upload.class, (messaging, msg) -> {
			String file = msg.getFileName();
			logger.trace("receiving data for {}: {}", file, this);
			return messaging.receiveBinaryStream()
					.streamTo(client.uploadSerial(file, msg.getOffset()))
					.thenCompose($ -> messaging.send(new UploadFinished()))
					.thenCompose($ -> messaging.sendEndOfStream())
					.thenRun(messaging::close)
					.whenComplete(uploadStage.recordStats())
					.thenRun(() -> logger.trace("finished receiving data for {}: {}", file, this))
					.toVoid();
		});

		onMessage(Download.class, (messaging, msg) -> {
			String fileName = msg.getFileName();
			return client.list(fileName)
					.thenCompose(list -> {
						if (list.isEmpty()) {
							return Stage.ofException(new RemoteFsException("File not found: " + fileName));
						}
						long size = list.get(0).getSize();
						long length = msg.getLength();
						long offset = msg.getOffset();

						String repr = fileName + "(size=" + size + (offset != 0 ? ", offset=" + offset : "") + (length != -1 ? ", length=" + length : "");
						logger.trace("requested file {}: {}", repr, this);

						if (offset > size) {
							return Stage.ofException(new RemoteFsException("Offset exceeds file size for " + repr));
						}
						if (length != -1 && offset + length > size) {
							return Stage.ofException(new RemoteFsException("Boundaries exceed file size for " + repr));
						}

						long fixedLength = length == -1 ? size - offset : length;

						return messaging.send(new DownloadSize(fixedLength))
								.thenCompose($ -> {
									logger.trace("sending data for {}: {}", repr, this);
									return client.downloadSerial(fileName, offset, fixedLength)
											.streamTo(messaging.sendBinaryStream())
											.thenRun(() -> logger.trace("finished sending data for {}: {}", repr, this));
								});
					})
					.whenComplete(downloadStage.recordStats());
		});
		simpleHandler(Move.class, Move::getChanges, FsClient::move, MoveFinished::new, moveStage);
		simpleHandler(Copy.class, Copy::getChanges, FsClient::copy, CopyFinished::new, copyStage);
		simpleHandler(List.class, List::getGlob, FsClient::list, ListFinished::new, listStage);
		simpleHandler(Delete.class, Delete::getGlob, FsClient::delete, DeleteFinished::new, deleteStage);
	}

	private <T extends FsCommand, E, R> void simpleHandler(Class<T> cls,
			Function<T, E> extractor, BiFunction<FsClient, E, Stage<R>> action,
			Function<R, FsResponse> res, StageStats stats) {
		onMessage(cls, (messaging, msg) -> action.apply(client, extractor.apply(msg))
				.thenCompose(item -> messaging.send(res.apply(item)))
				.thenCompose($ -> messaging.sendEndOfStream())
				.whenComplete(stats.recordStats()));
	}

	@FunctionalInterface
	private interface MessagingHandler<T extends FsCommand> {
		Stage<Void> onMessage(Messaging<FsCommand, FsResponse> messaging, T item);
	}

	@SuppressWarnings("unchecked")
	private <T extends FsCommand> void onMessage(Class<T> type, MessagingHandler<T> handler) {
		handlers.put(type, (MessagingHandler<FsCommand>) handler);
	}

	@Override
	public String toString() {
		return "RemoteFsServer(" + client + ')';
	}

	// region JMX
	@JmxAttribute
	public StageStats getUploadStage() {
		return uploadStage;
	}

	@JmxAttribute
	public StageStats getDownloadStage() {
		return downloadStage;
	}

	@JmxAttribute
	public StageStats getMoveStage() {
		return moveStage;
	}

	@JmxAttribute
	public StageStats getListStage() {
		return listStage;
	}

	@JmxAttribute
	public StageStats getDeleteStage() {
		return deleteStage;
	}

	@JmxAttribute
	public StageStats getHandleRequestStage() {
		return handleRequestStage;
	}
	// endregion
}
