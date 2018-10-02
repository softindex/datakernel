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
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.KeyPair;
import io.global.globalfs.api.CheckpointPositionStrategy;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.transformers.FrameSigner;
import io.global.globalfs.transformers.FrameVerifier;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * An adapter from {@link GlobalFsNode} with a set {@link GlobalFsName} to an {@link FsClient}.
 * Does frame signing and verification on upload-download using given keys and strategies.
 */
public final class RemoteFsAdapter implements FsClient {
	private final GlobalFsNode.GlobalFsFileSystem fs;
	private final KeyPair keys;
	private final CheckpointPositionStrategy checkpointPositionStrategy;

	// region creators
	public RemoteFsAdapter(GlobalFsNode node, GlobalFsName name, KeyPair keys, CheckpointPositionStrategy checkpointPositionStrategy) {
		this.fs = node.getFileSystem(name);
		this.keys = keys;
		this.checkpointPositionStrategy = checkpointPositionStrategy;
	}
	// endregion

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		return fs.upload(filename, offset == -1 ? 0 : offset)
				.thenApply(consumer -> {
					long offset1 = offset == -1 ? 0 : offset;
					return consumer
							.apply(new FrameSigner(offset1, checkpointPositionStrategy, keys.getPrivKey()));
				});
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return fs.download(filename, offset, length)
				.thenApply(supplier -> supplier.apply(new FrameVerifier(keys.getPubKey(), offset, length)));
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return fs.move(changes);
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return fs.copy(changes);
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return fs.list(glob)
				.thenApply(res -> res.stream()
						.map(meta -> new FileMetadata(meta.getPath().getPath(), meta.getSize(), meta.getRevision()))
						.collect(toList()));
	}

	@Override
	public Stage<Void> delete(String glob) {
		return fs.delete(glob);
	}
}
