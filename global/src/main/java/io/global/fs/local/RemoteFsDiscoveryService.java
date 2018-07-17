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

package io.global.fs.local;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.Tuple2;
import io.datakernel.util.TypeT;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.csp.binary.ByteBufsParser.ofDecoder;
import static io.global.ot.util.BinaryDataFormats2.REGISTRY;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class RemoteFsDiscoveryService extends RuntimeDiscoveryService implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsDiscoveryService.class);

	private static final String ANNOUNCEMENTS_FILE = "announcements.bin";
	private static final String SHARED_KEYS_FILE = "shared-keys.bin";

	private static final StructuredCodec<Tuple2<PubKey, SignedData<AnnounceData>>> ANNOUNCE_CODEC =
			REGISTRY.get(new TypeT<Tuple2<PubKey, SignedData<AnnounceData>>>() {});

	private static final ByteBufsParser<Tuple2<PubKey, SignedData<AnnounceData>>> ANNOUNCE_PARSER = ofDecoder(ANNOUNCE_CODEC);

	private static final StructuredCodec<Tuple2<PubKey, List<Tuple2<Hash, SignedData<SharedSimKey>>>>> SHARED_KEY_CODEC =
			REGISTRY.get(new TypeT<Tuple2<PubKey, List<Tuple2<Hash, SignedData<SharedSimKey>>>>>() {});

	private static final ByteBufsParser<Tuple2<PubKey, List<Tuple2<Hash, SignedData<SharedSimKey>>>>> SHARED_KEY_PARSER = ofDecoder(SHARED_KEY_CODEC);

	private final Eventloop eventloop;
	private final FsClient storage;

	public RemoteFsDiscoveryService(Eventloop eventloop, FsClient storage) {
		this.eventloop = eventloop;
		this.storage = storage;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private Promise<Void> loadAnnouncements() {
		return storage.download(ANNOUNCEMENTS_FILE)
				.thenComposeEx((supplier, e) -> {
					if (e != null) {
						logger.info("Failed to load announcements from " + ANNOUNCEMENTS_FILE, e);
						return Promise.complete();
					}
					return BinaryChannelSupplier.of(supplier)
							.parseStream(ANNOUNCE_PARSER)
							.streamTo(ChannelConsumer.of(AsyncConsumer.of(tuple -> announced.put(tuple.getValue1(), tuple.getValue2()))));
				});
	}

	private Promise<Void> storeAnnouncements() {
		return storage.upload(ANNOUNCEMENTS_FILE, ".temp")
				.thenCompose(consumer ->
						ChannelSupplier.ofStream(announced.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), entry.getValue())))
								.map(tuple -> encode(ANNOUNCE_CODEC, tuple))
								.streamTo(consumer));
	}

	private Promise<Void> loadSharedKeys() {
		return storage.download(SHARED_KEYS_FILE)
				.thenComposeEx((supplier, e) -> {
					if (e != null) {
						logger.info("Failed to load shared keys from " + SHARED_KEYS_FILE, e);
						return Promise.complete();
					}
					return BinaryChannelSupplier.of(supplier)
							.parseStream(SHARED_KEY_PARSER)
							.streamTo(ChannelConsumer.of(AsyncConsumer.of(tuple ->
									sharedKeys.put(tuple.getValue1(), tuple.getValue2().stream().collect(toMap(Tuple2::getValue1, Tuple2::getValue2))))));
				});
	}

	private Promise<Void> storeSharedKeys() {
		return storage.upload(ANNOUNCEMENTS_FILE, ".temp")
				.thenCompose(consumer ->
						ChannelSupplier.ofStream(sharedKeys.entrySet()
								.stream()
								.map(entry ->
										new Tuple2<>(entry.getKey(), entry.getValue().entrySet()
												.stream()
												.map(entry2 -> new Tuple2<>(entry2.getKey(), entry2.getValue()))
												.collect(toList()))))
								.map(tuple -> encode(SHARED_KEY_CODEC, tuple))
								.streamTo(consumer));
	}

	@Override
	public Promise<Void> start() {
		return Promises.all(loadAnnouncements(), loadSharedKeys());
	}

	@Override
	public Promise<Void> stop() {
		return Promises.all(storeAnnouncements(), storeSharedKeys());
	}
}
