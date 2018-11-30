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
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.TypeT;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.GlobalFsCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeWithSizePrefix;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static io.global.fs.util.BinaryDataFormats.readBuf;

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
				.thenCompose(supplier -> supplier
						.withEndOfStream(eos -> eos
								.thenComposeEx(($, e) -> {
									if (e == null) {
										return Promise.complete();
									}
									logger.warn("Failed to read checkpoint data for {}", filename);
									// TODO anton: make below exception constant
									return Promise.ofException(new StacklessException(RemoteFsCheckpointStorage.class, "Failed to read checkpoint data for " + filename));
								}))
						.toCollector(ByteBufQueue.collector()));
	}

	@Override
	public Promise<Void> store(String filename, SignedData<GlobalFsCheckpoint> checkpoint) {
		long pos = checkpoint.getValue().getPosition();
		return load(filename, pos)
				.thenComposeEx((existing, e) -> {
					if (e == null) {
						return checkpoint.equals(existing) ?
								Promise.complete() :
								Promise.ofException(new StacklessException(RemoteFsCheckpointStorage.class, "Trying to override existing checkpoint at " + pos));
					}
					return storage.getMetadata(filename)
							.thenCompose(m -> storage.upload(filename, m != null ? m.getSize() : 0))
							.thenCompose(ChannelSupplier.of(encodeWithSizePrefix(SIGNED_CHECKPOINT_CODEC, checkpoint))::streamTo);
				});
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
					return Promise.ofException(new StacklessException(RemoteFsCheckpointStorage.class, "No checkpoint found on position " + position));
				});
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
				});
	}
}

