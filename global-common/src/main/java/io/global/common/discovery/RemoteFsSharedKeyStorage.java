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

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.TypeT;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.function.BiFunction;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;

public class RemoteFsSharedKeyStorage implements SharedKeyStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsSharedKeyStorage.class);

	private static final StructuredCodec<SignedData<SharedSimKey>> SHARED_KEY_CODEC =
			REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});

	private final FsClient storage;

	public RemoteFsSharedKeyStorage(FsClient storage) {
		this.storage = storage;
	}

	private String getGlobFor(PubKey receiver) {
		return receiver.asString() + File.separatorChar + "*";
	}

	private String getFilenameFor(PubKey receiver, Hash hash) {
		return receiver.asString() + File.separatorChar + hash.asString();
	}

	@Override
	public Promise<Void> store(PubKey receiver, SignedData<SharedSimKey> signedSharedSimKey) {
		String file = getFilenameFor(receiver, signedSharedSimKey.getValue().getHash());
		return storage.delete(file)
				.thenCompose($ -> storage.upload(file, 0))
				.thenCompose(ChannelSupplier.of(encode(SHARED_KEY_CODEC, signedSharedSimKey))::streamTo)
				.whenComplete(toLogger(logger, TRACE, "store", receiver, signedSharedSimKey, this));
	}

	private static final BiFunction<ChannelSupplier<ByteBuf>, Throwable, Promise<SignedData<SharedSimKey>>> LOAD_SHARED_KEY =
			(supplier, e) ->
					e != null ?
							Promise.ofException(e == FILE_NOT_FOUND ? NO_SHARED_KEY : e) :
							supplier.toCollector(ByteBufQueue.collector())
									.thenCompose(buf -> {
										try {
											return Promise.of(decode(SHARED_KEY_CODEC, buf));
										} catch (ParseException e1) {
											return Promise.ofException(e1);
										}
									});

	@Override
	public Promise<SignedData<SharedSimKey>> load(PubKey receiver, Hash hash) {
		return storage.download(getFilenameFor(receiver, hash))
				.thenComposeEx(LOAD_SHARED_KEY)
				.whenComplete(toLogger(logger, TRACE, "load", receiver, hash, this));
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> loadAll(PubKey receiver) {
		return storage.list(getGlobFor(receiver))
				.thenCompose(files ->
						Promises.collectSequence(toList(),
								files.stream()
										.map(meta -> AsyncSupplier.cast(() ->
												storage.download(meta.getFilename())
														.thenComposeEx(LOAD_SHARED_KEY)))))
				.whenComplete(toLogger(logger, TRACE, "loadAll", receiver, this));
	}

	@Override
	public String toString() {
		return "RemoteFsSharedKeyStorage{storage=" + storage + '}';
	}
}
