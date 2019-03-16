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
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.RecyclingChannelConsumer;
import io.datakernel.csp.binary.ByteBufSerializer;
import io.datakernel.csp.net.Messaging;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.remotefs.RemoteFsCommands.*;
import io.datakernel.remotefs.RemoteFsResponses.*;

import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.datakernel.csp.binary.ByteBufSerializer.ofJsonCodec;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.remotefs.RemoteFsUtils.checkRange;
import static io.datakernel.remotefs.RemoteFsUtils.getErrorCode;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;

/**
 * An implementation of {@link AbstractServer} for RemoteFs.
 * It exposes some given {@link FsClient} to the Internet in pair with {@link RemoteFsClient}
 */
public final class RemoteFsServer extends AbstractServer<RemoteFsServer> {
	private static final ByteBufSerializer<FsCommand, FsResponse> SERIALIZER =
			ofJsonCodec(RemoteFsCommands.CODEC, RemoteFsResponses.CODEC);

	public static final StacklessException NO_HANDLER_FOR_MESSAGE = new StacklessException(RemoteFsServer.class, "No handler for received message type");

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

	public static RemoteFsServer create(Eventloop eventloop, Executor executor, Path storage) {
		return new RemoteFsServer(eventloop, LocalFsClient.create(eventloop, storage));
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
				.then(msg -> {
					if (msg == null) {
						logger.warn("unexpected end of stream: {}", this);
						messaging.close();
						return Promise.complete();
					}
					MessagingHandler<FsCommand> handler = handlers.get(msg.getClass());
					if (handler == null) {
						logger.warn("received a message with no associated handler, type: " + msg.getClass());
						return Promise.ofException(NO_HANDLER_FOR_MESSAGE);
					}
					return handler.onMessage(messaging, msg);
				})
				.acceptEx(handleRequestPromise.recordStats())
				.thenEx(($, e) -> {
					if (e == null) {
						return Promise.complete();
					}
					logger.warn("got an error while handling message (" + e + ") : " + this);
					return messaging.send(new ServerError(getErrorCode(e)))
							.then($2 -> messaging.sendEndOfStream())
							.accept($2 -> messaging.close());
				});
	}

	private void addHandlers() {
		onMessage(Upload.class, (messaging, msg) -> {
			String name = msg.getName();
			return client.upload(name, msg.getOffset(), msg.getRevision())
					.then(uploader -> {
						if (uploader instanceof RecyclingChannelConsumer) {
							return messaging.send(new UploadAck(false));
						}
						return messaging.send(new UploadAck(true))
								.then($ -> messaging.receiveBinaryStream()
										.streamTo(uploader));
					})
					.then($ -> messaging.send(new UploadFinished()))
					.then($ -> messaging.sendEndOfStream())
					.accept($ -> messaging.close())
					.acceptEx(uploadPromise.recordStats())
					.acceptEx(toLogger(logger, TRACE, "receiving data", msg, this))
					.toVoid();
		});

		onMessage(Download.class, (messaging, msg) -> {
			String name = msg.getName();
			return client.getMetadata(name)
					.then(meta -> {
						if (meta == null) {
							return Promise.ofException(FILE_NOT_FOUND);
						}
						long size = meta.getSize();
						long offset = msg.getOffset();
						long length = msg.getLength();

						checkRange(size, offset, length);

						long fixedLength = length == -1 ? size - offset : length;

						return messaging.send(new DownloadSize(fixedLength))
								.then($ ->
										ChannelSupplier.ofPromise(client.download(name, offset, fixedLength))
												.streamTo(messaging.sendBinaryStream()))
								.acceptEx(toLogger(logger, "sending data", meta, offset, fixedLength, this));
					})
					.acceptEx(downloadPromise.recordStats());
		});
		onMessage(Move.class, simpleHandler(msg -> client.move(msg.getName(), msg.getTarget(), msg.getTargetRevision(), msg.getRemoveRevision()), $ -> new MoveFinished(), movePromise));
		onMessage(Copy.class, simpleHandler(msg -> client.copy(msg.getName(), msg.getTarget(), msg.getRevision()), $ -> new CopyFinished(), copyPromise));
		onMessage(Delete.class, simpleHandler(msg -> client.delete(msg.getName(), msg.getRevision()), $ -> new DeleteFinished(), deletePromise));
		onMessage(List.class, simpleHandler(msg ->
						msg.needTombstones() ?
								client.listEntities(msg.getGlob()) :
								client.list(msg.getGlob()),
				ListFinished::new, listPromise));
	}

	private <T extends FsCommand, R> MessagingHandler<T> simpleHandler(Function<T, Promise<R>> action, Function<R, FsResponse> response, PromiseStats stats) {
		return (messaging, msg) -> action.apply(msg)
				.then(res -> messaging.send(response.apply(res)))
				.then($ -> messaging.sendEndOfStream())
				.acceptEx(stats.recordStats());
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
