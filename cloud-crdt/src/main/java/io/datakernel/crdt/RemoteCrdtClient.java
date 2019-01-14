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

package io.datakernel.crdt;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.crdt.CrdtMessaging.*;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.csp.process.ChannelDeserializer;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.net.SocketSettings;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.function.Function;

import static io.datakernel.crdt.CrdtMessaging.CrdtMessages.PING;
import static io.datakernel.crdt.CrdtMessaging.CrdtResponses.*;
import static io.datakernel.crdt.CrdtMessaging.*;
import static io.datakernel.csp.binary.ByteBufSerializer.ofJsonCodec;

public final class RemoteCrdtClient<K extends Comparable<K>, S> implements CrdtClient<K, S>, EventloopService, EventloopJmxMBeanEx {
	private final Eventloop eventloop;
	private final InetSocketAddress address;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<K> keySerializer;

	private SocketSettings socketSettings = SocketSettings.create();

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();
	// endregion

	//region creators
	private RemoteCrdtClient(Eventloop eventloop, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		this.eventloop = eventloop;
		this.address = address;
		this.serializer = serializer;

		keySerializer = serializer.getKeySerializer();
	}

	public static <K extends Comparable<K>, S> RemoteCrdtClient<K, S> create(Eventloop eventloop, InetSocketAddress address,
			CrdtDataSerializer<K, S> serializer) {
		return new RemoteCrdtClient<>(eventloop, address, serializer);
	}

	public static <K extends Comparable<K>, S> RemoteCrdtClient<K, S> create(Eventloop eventloop, InetSocketAddress address,
			BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new RemoteCrdtClient<>(eventloop, address, new CrdtDataSerializer<>(keySerializer, stateSerializer));
	}

	public RemoteCrdtClient<K, S> withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}
	//endregion

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect()
				.thenCompose(messaging ->
						messaging.send(CrdtMessages.UPLOAD)
								.thenApply($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.thenCompose($2 -> messaging.receive())
													.thenCompose(simpleHandler(UPLOAD_FINISHED)));
									return StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? uploadStats : uploadStatsDetailed)
													.transformWith(ChannelSerializer.create(serializer))
													.streamTo(consumer))
											.withLateBinding();
								}));
	}

	@Override
	public CrdtStreamSupplierWithToken<K, S> download(long token) {
		SettablePromise<Long> newToken = new SettablePromise<>();
		return new CrdtStreamSupplierWithToken<>(connect()
				.thenCompose(messaging -> messaging.send(new Download(token))
						.thenCompose($ -> messaging.receive())
						.thenCompose(response -> {
							if (response == null) {
								return Promise.ofException(new IllegalStateException("Unexpected end of stream"));
							}
							if (response.getClass() == DownloadToken.class) {
								return Promise.of(((DownloadToken) response).getToken());
							}
							if (response instanceof ServerError) {
								return Promise.ofException(new StacklessException(RemoteCrdtClient.class, ((ServerError) response).getMsg()));
							}
							return Promise.ofException(new IllegalStateException("Received message " + response + " instead of " + DownloadToken.class.getSimpleName()));
						})
						.whenComplete(newToken::set)
						.thenApply($ ->
								messaging.receiveBinaryStream()
										.transformWith(ChannelDeserializer.create(serializer))
										.transformWith(detailedStats ? downloadStats : downloadStatsDetailed)
										.withEndOfStream(eos -> eos
												.thenCompose($2 -> messaging.sendEndOfStream())
												.whenResult($2 -> messaging.close()))
										.withLateBinding())), newToken);
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return connect()
				.thenCompose(messaging ->
						messaging.send(CrdtMessages.REMOVE)
								.thenApply($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.thenCompose($2 -> messaging.receive())
													.thenCompose(simpleHandler(REMOVE_FINISHED)));
									return StreamConsumer.<K>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? removeStats : removeStatsDetailed)
													.transformWith(ChannelSerializer.create(keySerializer))
													.streamTo(consumer))
											.withLateBinding();
								}));
	}

	@Override
	public Promise<Void> ping() {
		return connect()
				.thenCompose(messaging -> messaging.send(PING)
						.thenCompose($ -> messaging.receive())
						.thenCompose(simpleHandler(PONG)));
	}

	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	private Function<CrdtResponse, Promise<Void>> simpleHandler(CrdtResponse expected) {
		return response -> {
			if (response == null) {
				return Promise.ofException(new IllegalStateException("Unexpected end of stream"));
			}
			if (response == expected) {
				return Promise.complete();
			}
			if (response instanceof ServerError) {
				return Promise.ofException(new StacklessException(RemoteCrdtClient.class, ((ServerError) response).getMsg()));
			}
			return Promise.ofException(new IllegalStateException("Received message " + response + " instead of " + expected));
		};
	}

	private Promise<MessagingWithBinaryStreaming<CrdtResponse, CrdtMessage>> connect() {
		return Promise.ofCallback(cb ->
				eventloop.connect(address, new ConnectCallback() {
					@Override
					public void onConnect(@NotNull SocketChannel channel) {
						AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, channel, socketSettings);
						cb.set(MessagingWithBinaryStreaming.create(socket, ofJsonCodec(RESPONSE_CODEC, MESSAGE_CODEC)));
					}

					@Override
					public void onException(@NotNull Throwable e) {
						cb.setException(e);
					}
				}));
	}

	// region JMX
	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public StreamStatsBasic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}
	// endregion
}
