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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.remotefs.RemoteFsCommands.*;
import io.datakernel.remotefs.RemoteFsResponses.*;
import io.datakernel.serial.net.ByteBufSerializer;
import io.datakernel.serial.net.ByteBufSerializers;
import io.datakernel.serial.net.Messaging;
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
	private static final ByteBufSerializer<FsCommand, FsResponse> SERIALIZER =
			ByteBufSerializers.ofJson(RemoteFsCommands.ADAPTER, RemoteFsResponses.ADAPTER);

	private final Map<Class<?>, MessagingHandler<FsCommand>> handlers = new HashMap<>();
	private final FsClient client;

	// region JMX
	private final PromiseStats handleRequestPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
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
						return Promise.complete();
					}
					MessagingHandler<FsCommand> handler = handlers.get(msg.getClass());
					if (handler == null) {
						return Promise.ofException(new Exception("no handler for " + msg + " " + this));
					}
					return handler.onMessage(messaging, msg);
				})
				.whenComplete(handleRequestPromise.recordStats())
				.thenComposeEx(($, e) -> {
					if (e == null) {
						return Promise.complete();
					}
					logger.warn("got an error while handling message (" + e + ") : " + this);
					String prefix = e.getClass() != StacklessException.class ? e.getClass().getSimpleName() + ": " : "";
					return messaging.send(new ServerError(prefix + e.getMessage()))
							.thenCompose($1 -> messaging.sendEndOfStream())
							.whenResult($1 -> messaging.close(e));
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
					.whenResult($ -> messaging.close())
					.whenComplete(uploadPromise.recordStats())
					.whenResult($ -> logger.trace("finished receiving data for {}: {}", file, this))
					.toVoid();
		});

		onMessage(Download.class, (messaging, msg) -> {
			String fileName = msg.getFileName();
			return client.list(fileName)
					.thenCompose(list -> {
						if (list.isEmpty()) {
							return Promise.ofException(new StacklessException(RemoteFsServer.class, "File not found: " + fileName));
						}
						long size = list.get(0).getSize();
						long length = msg.getLength();
						long offset = msg.getOffset();

						String repr = fileName + "(size=" + size + (offset != 0 ? ", offset=" + offset : "") + (length != -1 ? ", length=" + length : "");
						logger.trace("requested file {}: {}", repr, this);

						if (offset > size) {
							return Promise.ofException(new StacklessException(RemoteFsServer.class, "Offset exceeds file size for " + repr));
						}
						if (length != -1 && offset + length > size) {
							return Promise.ofException(new StacklessException(RemoteFsServer.class, "Boundaries exceed file size for " + repr));
						}

						long fixedLength = length == -1 ? size - offset : length;

						return messaging.send(new DownloadSize(fixedLength))
								.thenCompose($ -> {
									logger.trace("sending data for {}: {}", repr, this);
									return client.downloadSerial(fileName, offset, fixedLength)
											.streamTo(messaging.sendBinaryStream())
											.whenResult($1 -> logger.trace("finished sending data for {}: {}", repr, this));
								});
					})
					.whenComplete(downloadPromise.recordStats());
		});
		simpleHandler(Move.class, Move::getChanges, FsClient::move, MoveFinished::new, movePromise);
		simpleHandler(Copy.class, Copy::getChanges, FsClient::copy, CopyFinished::new, copyPromise);
		simpleHandler(List.class, List::getGlob, FsClient::list, ListFinished::new, listPromise);
		simpleHandler(Delete.class, Delete::getGlob, FsClient::delete, DeleteFinished::new, deletePromise);
	}

	private <T extends FsCommand, E, R> void simpleHandler(Class<T> cls,
			Function<T, E> extractor, BiFunction<FsClient, E, Promise<R>> action,
			Function<R, FsResponse> res, PromiseStats stats) {
		onMessage(cls, (messaging, msg) -> action.apply(client, extractor.apply(msg))
				.thenCompose(item -> messaging.send(res.apply(item)))
				.thenCompose($ -> messaging.sendEndOfStream())
				.whenComplete(stats.recordStats()));
	}

	@FunctionalInterface
	private interface MessagingHandler<T extends FsCommand> {
		Promise<Void> onMessage(Messaging<FsCommand, FsResponse> messaging, T item);
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
	public PromiseStats getUploadPromise() {
		return uploadPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadPromise() {
		return downloadPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getHandleRequestPromise() {
		return handleRequestPromise;
	}
	// endregion
}
