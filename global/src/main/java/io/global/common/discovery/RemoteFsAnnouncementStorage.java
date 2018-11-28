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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.global.common.BinaryDataFormats.REGISTRY;

public final class RemoteFsAnnouncementStorage implements AnnouncementStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsAnnouncementStorage.class);
	private static final StructuredCodec<SignedData<AnnounceData>> ANNOUNCEMENT_CODEC =
			REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});

	private final FsClient storage;

	public RemoteFsAnnouncementStorage(FsClient storage) {
		this.storage = storage;
	}

	private String getFilenameFor(PubKey space) {
		return space.asString();
	}

	@Override
	public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
		logger.trace("storing {}", announceData);
		String file = getFilenameFor(space);
		return storage.delete(file)
				.thenCompose($ -> storage.upload(file, 0))
				.thenCompose(ChannelSupplier.of(encode(ANNOUNCEMENT_CODEC, announceData))::streamTo);
	}

	@Override
	public Promise<SignedData<AnnounceData>> load(PubKey space) {
		return storage.download(getFilenameFor(space))
				.thenComposeEx((supplier, e) ->
						e != null ?
								Promise.ofException(e == FILE_NOT_FOUND ? NO_ANNOUNCEMENT : e) :
								supplier.toCollector(ByteBufQueue.collector()))
				.thenCompose(buf -> {
					try {
						return Promise.of(decode(ANNOUNCEMENT_CODEC, buf));
					} catch (ParseException e1) {
						return Promise.ofException(e1);
					}
				});
	}
}
