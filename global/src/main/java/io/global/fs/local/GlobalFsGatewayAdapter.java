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

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.*;
import io.global.fs.transformers.FrameSigner;
import io.global.fs.transformers.FrameVerifier;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class GlobalFsGatewayAdapter implements GlobalFsGateway, Initializable<GlobalFsGatewayAdapter> {
	private static final GlobalFsException UNKNOWN_KEY = new GlobalFsException(GlobalFsGatewayAdapter.class, "Unknown public key");

	private final GlobalFsNode node;
	private final Map<PubKey, PrivKey> keymap;
	private final CheckpointPositionStrategy checkpointPositionStrategy;

	public GlobalFsGatewayAdapter(GlobalFsNode node, Set<PrivKey> keys, CheckpointPositionStrategy checkpointPositionStrategy) {
		this.node = node;
		keymap = keys.stream().collect(toMap(PrivKey::computePubKey, Function.identity()));
		this.checkpointPositionStrategy = checkpointPositionStrategy;
	}

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(GlobalFsPath path, long offset) {
		PrivKey privKey = keymap.get(path.getPubKey());
		if (privKey == null) {
			return Stage.ofException(UNKNOWN_KEY);
		}
		return node.upload(path, offset)
				.thenApply(consumer -> consumer
						.apply(new FrameSigner(path.getFullPath(), offset == -1 ? 0 : offset, checkpointPositionStrategy, privKey)));
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(GlobalFsPath path, long offset, long limit) {
		return node.download(path, offset, limit)
				.thenApply(supplier -> supplier.apply(new FrameVerifier(path.getFullPath(), path.getPubKey(), offset, limit)));
	}

	@Override
	public Stage<Void> updateMetadata(PubKey pubKey, GlobalFsMetadata meta) {
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			return Stage.ofException(UNKNOWN_KEY);
		}
		return node.pushMetadata(pubKey, SignedData.sign(meta, privKey));
	}

	@Override
	public FsClient getFsDriver(GlobalFsSpace space, CurrentTimeProvider timeProvider) {
		PubKey pubKey = space.getPubKey();
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			throw new IllegalArgumentException("Cannot get remotefs adapter for space with unknown public key");
		}
		return new FsClient() {
			@Override
			public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
				return GlobalFsGatewayAdapter.this.upload(space.pathFor(filename), offset);
			}

			@Override
			public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
				return GlobalFsGatewayAdapter.this.download(space.pathFor(filename), offset, length);
			}

			@Override
			public Stage<Set<String>> move(Map<String, String> changes) {
				throw new UnsupportedOperationException("No file moves in GlobalFS yet");
			}

			@Override
			public Stage<Set<String>> copy(Map<String, String> changes) {
				throw new UnsupportedOperationException("No file copies in GlobalFS yet");
			}

			@Override
			public Stage<List<FileMetadata>> list(String glob) {
				return node.list(space, glob)
						.thenApply(res -> res.stream()
								.map(signedMeta -> {
									GlobalFsMetadata meta = signedMeta.getData();
									return new FileMetadata(meta.getPath(), meta.getSize(), meta.getRevision());
								})
								.collect(toList()));
			}

			@Override
			public Stage<Void> delete(String glob) {
				return node.list(space, glob)
						.thenCompose(list ->
								Stages.all(list.stream().map(signedData -> {
									GlobalFsMetadata meta = signedData.getData();
									GlobalFsMetadata removed = GlobalFsMetadata.ofRemoved(meta.getFs(), meta.getPath(), timeProvider.currentTimeMillis());
									return node.pushMetadata(pubKey, SignedData.sign(removed, privKey));
								})));
			}
		};
	}

	public static FsClient getFsDriver(GlobalFsNode node, KeyPair keys, String fsName, CheckpointPositionStrategy checkpointPositionStrategy) {
		return new GlobalFsGatewayAdapter(node, singleton(keys.getPrivKey()), checkpointPositionStrategy).getFsDriver(GlobalFsSpace.of(keys, fsName));
	}
}
