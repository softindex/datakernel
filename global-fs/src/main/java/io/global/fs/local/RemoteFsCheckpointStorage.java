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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.TypeT;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.GlobalFsCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeWithSizePrefix;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static io.global.fs.util.BinaryDataFormats.readBuf;
import static java.util.stream.Collectors.toList;

public final class RemoteFsCheckpointStorage implements CheckpointStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsCheckpointStorage.class);
	private static final StructuredCodec<SignedData<GlobalFsCheckpoint>> SIGNED_CHECKPOINT_CODEC =
			REGISTRY.get(new TypeT<SignedData<GlobalFsCheckpoint>>() {});

	private final FsClient storage;

	public RemoteFsCheckpointStorage(FsClient storage) {
		this.storage = storage;
	}

	private Promise<ByteBuf> download(String filename) {
		return storage.download(filename)
				.thenComposeEx((supplier, e) -> {
					if (e != null) {
						return Promise.ofException(e == FILE_NOT_FOUND ? NO_CHECKPOINT : e);
					}
					return supplier.toCollector(ByteBufQueue.collector());
				});
	}

	@Override
	public Promise<Void> store(String filename, SignedData<GlobalFsCheckpoint> signedCheckpoint) {
		GlobalFsCheckpoint checkpoint = signedCheckpoint.getValue();
		if (checkpoint.isTombstone()) {
			return storage.delete(filename)
					.thenCompose($ -> storage.upload(filename, 0))
					.thenCompose(ChannelSupplier.of(encodeWithSizePrefix(SIGNED_CHECKPOINT_CODEC, signedCheckpoint))::streamTo);
		}
		long pos = checkpoint.getPosition();
		return load(filename, pos)
				.thenComposeEx((existing, e) -> {
					if (e == null) {
						return signedCheckpoint.equals(existing) ?
								Promise.complete() :
								Promise.ofException(OVERRIDING);
					}
					return storage.getMetadata(filename)
							.thenCompose(m -> storage.upload(filename, m != null ? m.getSize() : 0))
							.thenCompose(ChannelSupplier.of(encodeWithSizePrefix(SIGNED_CHECKPOINT_CODEC, signedCheckpoint))::streamTo);
				})
				.whenComplete(toLogger(logger, TRACE, "store", filename, signedCheckpoint, this));
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> load(String filename, long position) {
		return download(filename)
				.thenCompose(buf -> {
					while (buf.canRead()) {
						try {
							SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, readBuf(buf));
							if (checkpoint.getValue().getPosition() == position) {
								buf.recycle();
								return Promise.of(checkpoint);
							}
						} catch (ParseException e) {
							buf.recycle();
							return Promise.ofException(e);
						}
					}
					buf.recycle();
					return Promise.ofException(NO_CHECKPOINT);
				})
				.whenComplete(toLogger(logger, TRACE, "load", filename, position, this));
	}

	@Override
	public Promise<List<String>> listMetaCheckpoints(String glob) {
		return storage.list(glob)
				.thenApply(list -> list.stream().map(FileMetadata::getFilename).collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "listMetaCheckpoints", glob, this));
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> loadMetaCheckpoint(String filename) {
		return download(filename)
				.thenCompose(buf -> {
					SignedData<GlobalFsCheckpoint> max = null;
					while (buf.canRead()) {
						try {
							SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, readBuf(buf));
							if (max == null || checkpoint.getValue().getPosition() > max.getValue().getPosition()) {
								max = checkpoint;
							}
						} catch (ParseException e) {
							buf.recycle();
							return Promise.ofException(e);
						}
					}
					buf.recycle();
					return max != null ? Promise.of(max) : Promise.ofException(NO_CHECKPOINT);
				})
				.whenComplete(toLogger(logger, TRACE, "loadMetaCheckpoints", filename, this));
	}

	@Override
	public Promise<long[]> loadIndex(String filename) {
		return download(filename)
				.thenCompose(buf -> {
					long[] array = new long[32];
					int size = 0;
					while (buf.canRead()) {
						try {
							SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, readBuf(buf));
							if (array.length == size) {
								array = Arrays.copyOf(array, size * 2);
							}
							array[size++] = checkpoint.getValue().getPosition();
						} catch (ParseException e) {
							buf.recycle();
							return Promise.ofException(e);
						}
					}
					buf.recycle();
					return Promise.of(Arrays.stream(array).limit(size).sorted().toArray());
				})
				.whenComplete(toLogger(logger, TRACE, "loadIndex", filename, this));
	}

	@Override
	public Promise<Void> drop(String filename) {
		return storage.delete(filename)
				.whenComplete(toLogger(logger, TRACE, "drop", filename, this));
	}

	@Override
	public String toString() {
		return "RemoteFsCheckpointStorage{storage=" + storage + '}';
	}
}

