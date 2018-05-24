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
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.StageStats;
import io.datakernel.remotefs.RemoteFsCommands.*;
import io.datakernel.remotefs.RemoteFsResponses.*;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static io.datakernel.stream.net.MessagingSerializers.ofJson;

/**
 * An implementation of {@link AbstractServer} for RemoteFs.
 * It exposes some given {@link FsClient} to the Internet in pair with {@link RemoteFsClient}
 */
public final class RemoteFsServer extends AbstractServer<RemoteFsServer> {
	private static final MessagingSerializer<FsCommand, FsResponse> SERIALIZER =
		ofJson(RemoteFsCommands.ADAPTER, RemoteFsResponses.ADAPTER);

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
	protected EventHandler createSocketHandler(AsyncTcpSocket socket) {
		MessagingWithBinaryStreaming<FsCommand, FsResponse> messaging = MessagingWithBinaryStreaming.create(socket, SERIALIZER);
		messaging.receive()
			.thenCompose(msg -> {
				if (msg == null) {
					logger.warn("unexpected end of stream: {}", this);
					messaging.close();
					return Stage.of(null);
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
					return Stage.of(null);
				}
				logger.warn("got an error while handling message (" + err + ") : " + this);
				String prefix = err.getClass() != RemoteFsException.class ? err.getClass().getSimpleName() + ": " : "";
				return messaging.send(new ServerError(prefix + err.getMessage()))
					.thenCompose($2 -> messaging.sendEndOfStream())
					.thenRun(messaging::close);
			});
		return messaging;
	}

	private void addHandlers() {
		onMessage(Upload.class, (messaging, msg) -> {
			String file = msg.getFileName();
			logger.trace("receiving data for {}: {}", file, this);
			return messaging.receiveBinaryStream().streamTo(client.uploadStream(file, msg.getOffset())).getConsumerResult()
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
							return client.downloadStream(fileName, offset, fixedLength).streamTo(messaging.sendBinaryStream())
								.getConsumerResult()
								.thenRun(() -> logger.trace("finished sending data for {}: {}", repr, this));
						});
				})
				.whenComplete(downloadStage.recordStats());
		});
		onMessage(Move.class, (messaging, msg) -> client.move(msg.getChanges()).thenCompose(simple(messaging, MoveFinished::new)).whenComplete(moveStage.recordStats()));
		onMessage(Copy.class, (messaging, msg) -> client.copy(msg.getChanges()).thenCompose(simple(messaging, CopyFinished::new)).whenComplete(copyStage.recordStats()));
		onMessage(List.class, (messaging, msg) -> client.list(msg.getGlob()).thenCompose(simple(messaging, ListFinished::new)).whenComplete(listStage.recordStats()));
		onMessage(Delete.class, (messaging, msg) -> client.delete(msg.getGlob()).thenCompose(simple(messaging, DeleteFinished::new)).whenComplete(deleteStage.recordStats()));
	}

	private static <T> Function<T, Stage<Void>> simple(Messaging<?, FsResponse> messaging, Function<T, FsResponse> res) {
		return item -> messaging.send(res.apply(item)).thenCompose($ -> messaging.sendEndOfStream());
	}

	@FunctionalInterface
	private interface MessagingHandler<R extends FsCommand> {
		Stage<Void> onMessage(Messaging<FsCommand, FsResponse> messaging, R item);
	}

	@SuppressWarnings("unchecked")
	private <R extends FsCommand> void onMessage(Class<R> type, MessagingHandler<R> handler) {
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
