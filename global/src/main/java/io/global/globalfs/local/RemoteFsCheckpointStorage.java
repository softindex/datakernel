/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.local;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.SignedData;
import io.global.globalfs.api.CheckpointStorage;
import io.global.globalfs.api.GlobalFsCheckpoint;
import io.global.globalfs.api.GlobalFsException;
import io.global.globalsync.util.BinaryDataFormats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class RemoteFsCheckpointStorage implements CheckpointStorage {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsCheckpointStorage.class);

	private final FsClient fsClient;

	// region creators
	public RemoteFsCheckpointStorage(FsClient fsClient) {
		this.fsClient = fsClient;
	}
	// endregion

	private Stage<ByteBuf> download(String filename) {
		return fsClient.download(filename)
				.thenComposeEx((supplier, e) -> {
					if (e != null) {
						logger.warn("Failed to read checkpoint data for {}", filename, e);
						return Stage.of(null);
					}
					return supplier
							.withEndOfStream(eos -> eos
									.thenComposeEx(($, e2) -> {
										if (e2 != null) {
											return Stage.<Void>ofException(new GlobalFsException("Failed to read checkpoint data for {}"));
										}
										return Stage.of(null);
									}))
							.toCollector(ByteBufQueue.collector());
				});
	}

	@Override
	public Stage<long[]> getCheckpoints(String filename) {
		return download(filename)
				.thenCompose(buf -> {
					if (buf == null) {
						return Stage.of(new long[]{0});
					}
					long[] array = new long[32];
					int size = 0;
					while (buf.canRead()) {
						byte[] bytes = BinaryDataFormats.readBytes(buf);
						try {
							SignedData<GlobalFsCheckpoint> checkpoint = SignedData.ofBytes(bytes, GlobalFsCheckpoint::ofBytes);
							if (array.length == size) {
								array = Arrays.copyOf(array, size * 2);
							}
							array[size++] = checkpoint.getData().getPosition();
						} catch (ParseException e) {
							return Stage.ofException(e);
						}
					}
					return Stage.of(Arrays.stream(array).limit(size).sorted().toArray());
				});
	}

	@Override
	public Stage<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position) {
		return download(filename)
				.thenCompose(buf -> {
					if (buf == null) {
						return Stage.of(null);
					}
					while (buf.canRead()) {
						byte[] bytes = BinaryDataFormats.readBytes(buf);
						try {
							SignedData<GlobalFsCheckpoint> checkpoint = SignedData.ofBytes(bytes, GlobalFsCheckpoint::ofBytes);
							if (checkpoint.getData().getPosition() == position) {
								return Stage.of(checkpoint);
							}
						} catch (ParseException e) {
							return Stage.ofException(e);
						}
					}
					return Stage.of(null);
				});
	}

	@Override
	public Stage<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint) {
		return fsClient.getMetadata(filename)
				.thenCompose(m -> fsClient.upload(filename, m != null ? m.getSize() : 0))
				.thenCompose(consumer -> {
					byte[] bytes = checkpoint.toBytes();
					ByteBuf buf = ByteBufPool.allocate(bytes.length + 5);
					BinaryDataFormats.writeBytes(buf, bytes);
					return SerialSupplier.of(buf).streamTo(consumer);
				});
	}
}

