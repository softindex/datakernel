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

package io.global.common.discovery;

import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.remotefs.FsClient;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.async.util.LogUtils.Level.TRACE;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.global.common.BinaryDataFormats.REGISTRY;

public final class RemoteFsAnnouncementStorage implements AnnouncementStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsAnnouncementStorage.class);
	private static final StructuredCodec<PubKey> PUB_KEY_CODEC = REGISTRY.get(PubKey.class);
	private static final StructuredCodec<SignedData<AnnounceData>> ANNOUNCEMENT_CODEC =
			REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});
	private static final String PUB_KEY_PREFIX = "pubKey-";

	private final FsClient storage;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public RemoteFsAnnouncementStorage(FsClient storage) {
		this.storage = storage;
	}

	private String getFilenameFor(PubKey space) {
		return space.asString();
	}

	@Override
	public Promise<Void> store(PubKey space, SignedData<AnnounceData> signedAnnounceData) {
		String file = getFilenameFor(space);
		return storage.upload(file, 0, now.currentTimeMillis())
				.then(ChannelSupplier.of(encode(ANNOUNCEMENT_CODEC, signedAnnounceData))::streamTo)
				.then($ -> storage.upload(PUB_KEY_PREFIX + file))
				.then(ChannelSupplier.of(encode(PUB_KEY_CODEC, space))::streamTo)
				.whenComplete(toLogger(logger, TRACE, "store", signedAnnounceData, this));
	}

	@Override
	public Promise<@Nullable SignedData<AnnounceData>> load(PubKey space) {
		return doLoad(getFilenameFor(space))
				.whenComplete(toLogger(logger, TRACE, "load", space, this));
	}

	@Override
	public Promise<Map<PubKey, SignedData<AnnounceData>>> loadAll() {
		return storage.list(PUB_KEY_PREFIX + "*")
				.then(pubKeyFiles -> Promises.toList(pubKeyFiles.stream()
						.map(file -> storage.download(file.getName())
								.then(supplier -> supplier.toCollector(ByteBufQueue.collector())))))
				.then(pubKeysBufs -> Promises.toList(pubKeysBufs.stream()
						.map(buf -> {
							if (buf == null || !buf.canRead()) {
								return Promise.of(null);
							}
							try {
								return Promise.of(decode(PUB_KEY_CODEC, buf.slice()));
							} catch (ParseException e) {
								return Promise.ofException(e);
							} finally {
								buf.recycle();
							}
						})))
				.then(pubKeys -> {
					HashMap<PubKey, SignedData<AnnounceData>> result = new HashMap<>();
					return Promises.all(
							pubKeys.stream()
									.map(pubKey -> load(pubKey)
											.map(data -> {
												result.put(pubKey, data);
												return data;
											})
									))
							.map($ -> result);
				});
	}

	private Promise<SignedData<AnnounceData>> doLoad(String filename) {
		return storage.download(filename)
				.thenEx((supplier, e) -> {
					if (e == null) {
						return supplier.toCollector(ByteBufQueue.collector());
					}
					return e == FILE_NOT_FOUND ? Promise.of(null) : Promise.ofException(e);
				})
				.then(buf -> {
					if (buf == null || !buf.canRead()) {
						return Promise.of(null);
					}
					try {
						return Promise.of(decode(ANNOUNCEMENT_CODEC, buf.slice()));
					} catch (ParseException e2) {
						return Promise.ofException(e2);
					} finally {
						buf.recycle();
					}
				});
	}

	@Override
	public String toString() {
		return "RemoteFsAnnouncementStorage{storage=" + storage + '}';
	}
}
