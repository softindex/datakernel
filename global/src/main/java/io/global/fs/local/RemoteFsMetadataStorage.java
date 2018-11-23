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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.TypeT;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.MetadataStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiFunction;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;

public class RemoteFsMetadataStorage implements MetadataStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsMetadataStorage.class);
	private static final StructuredCodec<SignedData<GlobalFsMetadata>> SIGNED_METADATA_CODEC = REGISTRY.get(new TypeT<SignedData<GlobalFsMetadata>>() {});

	private final FsClient storage;

	public RemoteFsMetadataStorage(FsClient storage) {
		this.storage = storage;
	}

	@Override
	public Promise<Void> store(SignedData<GlobalFsMetadata> signedMetadata) {
		logger.trace("storing {}", signedMetadata);
		String path = signedMetadata.getValue().getFilename();
		return storage.deleteSingle(path)
				.thenCompose($ -> storage.upload(path, 0)) // offset 0 because this file could be concurrently uploaded atst
				.thenCompose(ChannelSupplier.of(encode(SIGNED_METADATA_CODEC, signedMetadata))::streamTo);
	}

	private static final BiFunction<ChannelSupplier<ByteBuf>, Throwable, Promise<SignedData<GlobalFsMetadata>>> LOAD_METADATA = (supplier, e) ->
			e != null ?
					Promise.ofException(e == FILE_NOT_FOUND ? NO_METADATA : e) :
					supplier.toCollector(ByteBufQueue.collector())
							.thenCompose(buf -> {
								try {
									SignedData<GlobalFsMetadata> signedMetadata = decode(SIGNED_METADATA_CODEC, buf);
									logger.trace("loading {}", signedMetadata);
									return Promise.of(signedMetadata);
								} catch (ParseException e2) {
									return Promise.ofException(e2);
								}
							});

	@Override
	public Promise<SignedData<GlobalFsMetadata>> load(String filename) {
		return storage.download(filename).thenComposeEx(LOAD_METADATA);
	}

	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> loadAll(String glob) {
		return storage.list(glob)
				.thenCompose(files ->
						Promises.collectSequence(toList(), files.stream()
								.map(meta -> storage.download(meta.getFilename())
										.thenComposeEx(LOAD_METADATA))));
	}
}
