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
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.ot.util.BinaryDataFormats2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static io.global.ot.util.BinaryDataFormats2.*;

public final class RemoteFsCheckpointStorage implements CheckpointStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsCheckpointStorage.class);
	private static final StructuredCodec<SignedData<GlobalFsCheckpoint>> SIGNED_CHECKPOINT_CODEC = REGISTRY.get(SignedData.class, GlobalFsCheckpoint.class);

	private final FsClient fsClient;

	// region creators
	public RemoteFsCheckpointStorage(FsClient fsClient) {
		this.fsClient = fsClient;
	}
	// endregion

	private Promise<ByteBuf> download(String filename) {
		return fsClient.download(filename)
				.thenCompose(supplier -> supplier
						.withEndOfStream(eos -> eos
								.thenComposeEx(($, e) -> {
									if (e == null) {
										return Promise.complete();
									}
									logger.warn("Failed to read checkpoint data for {}", filename);
									return Promise.ofException(new StacklessException(RemoteFsCheckpointStorage.class, "Failed to read checkpoint data for " + filename));
								}))
						.toCollector(ByteBufQueue.collector()));
	}

	@Override
	public Promise<long[]> getCheckpoints(String filename) {
		return download(filename)
				.thenCompose(buf -> {
					long[] array = new long[32];
					int size = 0;
					while (buf.canRead()) {
						try {
							byte[] bytes = readBytes(buf);
							SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, bytes);
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

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position) {
		return download(filename)
				.thenCompose(buf -> {
					if (buf == null) {
						return Promise.of(null);
					}
					while (buf.canRead()) {
						try {
							byte[] bytes = readBytes(buf);
							SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, bytes);
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
					return Promise.ofException(new StacklessException(CheckpointStorage.class, "No checkpoint found on position " + position));
				});
	}

	@Override
	public Promise<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint) {
		long pos = checkpoint.getValue().getPosition();
		return loadCheckpoint(filename, pos)
				.thenComposeEx((existing, e) -> {
					if (e == null) {
						return checkpoint.equals(existing) ?
								Promise.complete() :
								Promise.ofException(new StacklessException(CheckpointStorage.class, "Trying to override existing checkpoint at " + pos));
					}
					return fsClient.getMetadata(filename)
							.thenCompose(m -> fsClient.upload(filename, m != null ? m.getSize() : 0))
							.thenCompose(consumer -> {
								byte[] bytes = checkpoint.getBytes();
								ByteBuf buf = BinaryDataFormats2.writeBytes(bytes);
								return SerialSupplier.of(buf).streamTo(consumer);
							});
				});
	}

}

