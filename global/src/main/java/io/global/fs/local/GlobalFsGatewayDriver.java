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
import io.datakernel.exception.StacklessException;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.file.FileUtils.isWildcard;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class GlobalFsGatewayDriver implements GlobalFsGateway, Initializable<GlobalFsGatewayDriver> {
	private static final StacklessException UNKNOWN_KEY = new StacklessException(GlobalFsGatewayDriver.class, "Unknown public key");

	private final GlobalFsNode node;
	private final Map<PubKey, PrivKey> keymap;
	private final CheckpointPositionStrategy checkpointPositionStrategy;
	private final CurrentTimeProvider timeProvider;

	private GlobalFsGatewayDriver(GlobalFsNode node, Map<PubKey, PrivKey> keymap, CheckpointPositionStrategy checkpointPositionStrategy) {
		this.node = node;
		this.keymap = keymap;
		this.checkpointPositionStrategy = checkpointPositionStrategy;
		timeProvider = CurrentTimeProvider.ofSystem();
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, Map<PubKey, PrivKey> keymap, CheckpointPositionStrategy checkpointPositionStrategy) {
		return new GlobalFsGatewayDriver(node, keymap, checkpointPositionStrategy);
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, Set<PrivKey> keys, CheckpointPositionStrategy checkpointPositionStrategy) {
		return new GlobalFsGatewayDriver(node, keys.stream().collect(toMap(PrivKey::computePubKey, Function.identity())), checkpointPositionStrategy);
	}

	public static GlobalFsGatewayDriver createFromPairs(GlobalFsNode node, Set<KeyPair> keys, CheckpointPositionStrategy checkpointPositionStrategy) {
		return new GlobalFsGatewayDriver(node, keys.stream().collect(toMap(KeyPair::getPubKey, KeyPair::getPrivKey)), checkpointPositionStrategy);
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, PrivKey key, CheckpointPositionStrategy checkpointPositionStrategy) {
		Map<PubKey, PrivKey> map = new HashMap<>();
		map.put(key.computePubKey(), key);
		return new GlobalFsGatewayDriver(node, map, checkpointPositionStrategy);
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, KeyPair keys, CheckpointPositionStrategy checkpointPositionStrategy) {
		Map<PubKey, PrivKey> map = new HashMap<>();
		map.put(keys.getPubKey(), keys.getPrivKey());
		return new GlobalFsGatewayDriver(node, map, checkpointPositionStrategy);
	}

	public static FsClient createFsAdapter(GlobalFsNode node, KeyPair keys, String fsName, CheckpointPositionStrategy checkpointPositionStrategy) {
		return create(node, keys, checkpointPositionStrategy).createFsAdapter(GlobalFsSpace.of(keys, fsName));
	}

	public static FsClient createFsAdapter(GlobalFsNode node, PrivKey key, String fsName, CheckpointPositionStrategy checkpointPositionStrategy) {
		KeyPair keys = key.computeKeys();
		return create(node, keys, checkpointPositionStrategy).createFsAdapter(GlobalFsSpace.of(keys, fsName));
	}

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(GlobalFsPath path, long offset) {
		PubKey pubKey = path.getPubKey();
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			return Stage.ofException(UNKNOWN_KEY);
		}
		long[] size = {0};
		return node.upload(path, offset)
				.thenApply(consumer -> consumer
						.apply(new FrameSigner(path.getFullPath(), offset == -1 ? 0 : offset, checkpointPositionStrategy, privKey))
						.peek(buf -> size[0] += buf.readRemaining())
						.withAcknowledgement(ack -> ack
								.thenCompose($ -> {
									GlobalFsMetadata meta = GlobalFsMetadata.of(path.getFs(), path.getPath(), size[0], timeProvider.currentTimeMillis());
									return node.pushMetadata(pubKey, SignedData.sign(meta, privKey));
								})));
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(GlobalFsPath path, long offset, long limit) {
		return node.download(path, offset, limit)
				.thenApply(supplier -> supplier.apply(new FrameVerifier(path.getFullPath(), path.getPubKey(), offset, limit)));
	}

	@Override
	public Stage<List<GlobalFsMetadata>> list(GlobalFsSpace space, String glob) {
		PubKey pubKey = space.getPubKey();
		return node.list(space, glob)
				.thenApply(res -> res.stream()
						.filter(signedMeta -> signedMeta.verify(pubKey))
						.map(SignedData::getData)
						.collect(toList()));
	}

	@Override
	public Stage<Void> delete(GlobalFsPath path) {
		PubKey pubKey = path.getPubKey();
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			return Stage.ofException(UNKNOWN_KEY);
		}
		return node.pushMetadata(pubKey, SignedData.sign(GlobalFsMetadata.ofRemoved(path.getFs(), path.getPath(), timeProvider.currentTimeMillis()), privKey));
	}

	@Override
	public Stage<Void> delete(GlobalFsSpace space, String glob) {
		PubKey pubKey = space.getPubKey();
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			return Stage.ofException(UNKNOWN_KEY);
		}
		if (!isWildcard(glob)) {
			return delete(space.pathFor(glob));
		}
		return node.list(space, glob)
				.thenCompose(list ->
						Stages.all(list.stream()
								.filter(signedMeta -> signedMeta.verify(pubKey))
								.map(signedMeta ->
										node.pushMetadata(pubKey, SignedData.sign(signedMeta.getData().toRemoved(), privKey)))));
	}

	@Override
	public FsClient createFsAdapter(GlobalFsSpace space, CurrentTimeProvider timeProvider) {
		if (!keymap.containsKey(space.getPubKey())) {
			throw new IllegalArgumentException("Cannot get remotefs adapter for space with unknown public key");
		}
		return GlobalFsGateway.super.createFsAdapter(space, timeProvider);
	}
}
