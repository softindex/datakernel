/*
 * Copyright (C) 2015-2020 SoftIndex LLC.
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

package io.datakernel.kv.remote;

import io.datakernel.async.service.EventloopService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.kv.CrdtData;
import io.datakernel.kv.CrdtClient;
import io.datakernel.kv.CrdtData.CrdtDataSerializer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.datastream.stats.StreamStats;
import io.datakernel.datastream.stats.StreamStatsBasic;
import io.datakernel.datastream.stats.StreamStatsDetailed;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.eventloop.net.SocketSettings;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.net.AsyncTcpSocketNio;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.function.Function;

import static io.datakernel.kv.KvMessaging.*;
import static io.datakernel.kv.KvMessaging.KvRequests.PING;
import static io.datakernel.kv.KvMessaging.KvResponses.*;
import static io.datakernel.csp.binary.ByteBufSerializer.ofJsonCodec;

public final class CrdtRemoteClient<K extends Comparable<K>, S> implements CrdtClient<K, S>, EventloopService, EventloopJmxMBeanEx {
	private final Eventloop eventloop;
	private final InetSocketAddress address;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<K> keySerializer;

	private SocketSettings socketSettings = SocketSettings.createDefault();

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
	private CrdtRemoteClient(Eventloop eventloop, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		this.eventloop = eventloop;
		this.address = address;
		this.serializer = serializer;

		keySerializer = serializer.getKeySerializer();
	}

	public static <K extends Comparable<K>, S> CrdtRemoteClient<K, S> create(Eventloop eventloop, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		return new CrdtRemoteClient<>(eventloop, address, serializer);
	}

	public static <K extends Comparable<K>, S> CrdtRemoteClient<K, S> create(Eventloop eventloop, InetSocketAddress address, BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new CrdtRemoteClient<>(eventloop, address, new CrdtDataSerializer<>(keySerializer, stateSerializer));
	}

	public CrdtRemoteClient<K, S> withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}
	//endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect()
				.then(messaging ->
						messaging.send(KvRequests.UPLOAD)
								.map($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then($2 -> messaging.receive())
													.then(simpleHandler(UPLOAD_FINISHED)));
									return StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? uploadStats : uploadStatsDetailed)
													.transformWith(ChannelSerializer.create(serializer))
													.streamTo(consumer))
											.withLateBinding();
								}));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long revision) {
		return connect()
				.then(messaging -> messaging.send(new Download(revision))
						.then($ -> messaging.receive())
						.then(simpleHandler(DOWNLOAD_STARTED))
						.map($ ->
								messaging.receiveBinaryStream()
										.transformWith(ChannelDeserializer.create(serializer))
										.transformWith(detailedStats ? downloadStats : downloadStatsDetailed)
										.withEndOfStream(eos -> eos
												.then($2 -> messaging.sendEndOfStream())
												.whenResult($2 -> messaging.close()))
										.withLateBinding()));
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return connect()
				.then(messaging ->
						messaging.send(KvRequests.REMOVE)
								.map($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then($2 -> messaging.receive())
													.then(simpleHandler(REMOVE_FINISHED)));
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
				.then(messaging -> messaging.send(PING)
						.then($ -> messaging.receive())
						.then(simpleHandler(PONG)));
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

	private Function<KvResponse, Promise<Void>> simpleHandler(KvResponse expected) {
		return response -> {
			if (response == null) {
				return Promise.ofException(new IllegalStateException("Unexpected end of stream"));
			}
			if (response == expected) {
				return Promise.complete();
			}
			if (response instanceof ServerError) {
				return Promise.ofException(new StacklessException(CrdtRemoteClient.class, ((ServerError) response).getMsg()));
			}
			return Promise.ofException(new IllegalStateException("Received message " + response + " instead of " + expected));
		};
	}

	private Promise<MessagingWithBinaryStreaming<KvResponse, KvRequest>> connect() {
		return AsyncTcpSocketNio.connect(address, null, socketSettings)
				.map(socket -> MessagingWithBinaryStreaming.create(socket, ofJsonCodec(RESPONSE_CODEC, REQUEST_CODEC)));
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
