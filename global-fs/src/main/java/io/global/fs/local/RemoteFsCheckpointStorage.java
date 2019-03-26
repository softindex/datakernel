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
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.TypeT;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.GlobalFsCheckpoint;
import org.jetbrains.annotations.Nullable;
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

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public RemoteFsCheckpointStorage(FsClient storage) {
		this.storage = storage;
	}

	private Promise<ByteBuf> download(String filename) {
		return storage.download(filename)
				.then(supplier -> supplier.toCollector(ByteBufQueue.collector()));
	}

	@Override
	public Promise<Void> store(String filename, SignedData<GlobalFsCheckpoint> signedCheckpoint) {
		GlobalFsCheckpoint checkpoint = signedCheckpoint.getValue();
		if (checkpoint.isTombstone()) {
			return ChannelSupplier.of(encodeWithSizePrefix(SIGNED_CHECKPOINT_CODEC, signedCheckpoint))
					.streamTo(storage.upload(filename, 0, now.currentTimeMillis()));
		}
		return load(filename, checkpoint.getPosition())
				.then(existing -> {
					if (existing == null) {
						return ChannelSupplier.of(encodeWithSizePrefix(SIGNED_CHECKPOINT_CODEC, signedCheckpoint))
								.streamTo(storage.append(filename));
					}
					return signedCheckpoint.equals(existing) ?
							Promise.complete() :
							Promise.ofException(OVERRIDING);
				})
				.whenComplete(toLogger(logger, TRACE, "store", filename, signedCheckpoint, this));
	}

	@Override
	public Promise<@Nullable SignedData<GlobalFsCheckpoint>> load(String filename, long position) {
		return download(filename)
				.thenEx((buf, e) -> {
					if (e != null) {
						return e == FILE_NOT_FOUND ?
								Promise.of(null) :
								Promise.ofException(e);
					}
					try {
						while (buf.canRead()) {
							try {
								SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, readBuf(buf));
								if (checkpoint.getValue().getPosition() == position) {
									return Promise.of(checkpoint);
								}
							} catch (ParseException e2) {
								return Promise.ofException(e2);
							}
						}
						return Promise.of(null);
					} finally {
						buf.recycle();
					}
				})
				.whenComplete(toLogger(logger, TRACE, "load", filename, position, this));
	}

	@Override
	public Promise<List<String>> listMetaCheckpoints(String glob) {
		return storage.list(glob)
				.map(list -> list.stream().map(FileMetadata::getName).collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "listMetaCheckpoints", glob, this));
	}

	@Override
	public Promise<@Nullable SignedData<GlobalFsCheckpoint>> loadMetaCheckpoint(String filename) {
		return download(filename)
				.thenEx((buf, e) -> {
					if (e != null) {
						return e == FILE_NOT_FOUND ?
								Promise.of(null) :
								Promise.ofException(e);
					}
					try {
						SignedData<GlobalFsCheckpoint> max = null;
						while (buf.canRead()) {
							try {
								SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, readBuf(buf));
								if (max == null || checkpoint.getValue().getPosition() > max.getValue().getPosition()) {
									max = checkpoint;
								}
							} catch (ParseException e2) {
								return Promise.ofException(e2);
							}
						}
						return Promise.of(max);
					} finally {
						buf.recycle();
					}
				})
				.whenComplete(toLogger(logger, TRACE, "loadMetaCheckpoints", filename, this));
	}

	@Override
	public Promise<long[]> loadIndex(String filename) {
		return download(filename)
				.thenEx((buf, e) -> {
					if (e != null) {
						return e == FILE_NOT_FOUND ?
								Promise.of(new long[0]) :
								Promise.ofException(e);
					}
					try {
						long[] array = new long[32];
						int size = 0;
						while (buf.canRead()) {
							try {
								SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, readBuf(buf));
								if (array.length == size) {
									array = Arrays.copyOf(array, size * 2);
								}
								array[size++] = checkpoint.getValue().getPosition();
							} catch (ParseException e2) {
								return Promise.ofException(e2);
							}
						}
						return Promise.of(Arrays.stream(array).limit(size).sorted().toArray());
					} finally {
						buf.recycle();
					}
				})
				.whenComplete(toLogger(logger, TRACE, "loadIndex", filename, this));
	}

	@Override
	public Promise<Void> drop(String filename, long revision) {
		return loadMetaCheckpoint(filename)
				.then(meta -> {
					if (meta != null && meta.getValue().getRevision() < revision) {
						return storage.truncate(filename, now.currentTimeMillis());
					}
					return Promise.complete();
				})
				.whenComplete(toLogger(logger, TRACE, "drop", filename, revision, this));
	}

	@Override
	public String toString() {
		return "RemoteFsCheckpointStorage{storage=" + storage + '}';
	}
}

