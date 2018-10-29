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
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.SerialByteBufCutter;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.*;
import io.global.fs.api.*;
import io.global.fs.transformers.FrameSigner;
import io.global.fs.transformers.FrameVerifier;
import org.spongycastle.crypto.digests.SHA256Digest;

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
	private final CheckpointPosStrategy checkpointPosStrategy;
	private final CurrentTimeProvider timeProvider;

	private GlobalFsGatewayDriver(GlobalFsNode node, Map<PubKey, PrivKey> keymap, CheckpointPosStrategy checkpointPosStrategy) {
		this.node = node;
		this.keymap = keymap;
		this.checkpointPosStrategy = checkpointPosStrategy;
		timeProvider = CurrentTimeProvider.ofSystem();
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, Map<PubKey, PrivKey> keymap, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsGatewayDriver(node, keymap, checkpointPosStrategy);
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, Set<PrivKey> keys, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsGatewayDriver(node, keys.stream().collect(toMap(PrivKey::computePubKey, Function.identity())), checkpointPosStrategy);
	}

	public static GlobalFsGatewayDriver createFromPairs(GlobalFsNode node, Set<KeyPair> keys, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsGatewayDriver(node, keys.stream().collect(toMap(KeyPair::getPubKey, KeyPair::getPrivKey)), checkpointPosStrategy);
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, PrivKey key, CheckpointPosStrategy checkpointPosStrategy) {
		Map<PubKey, PrivKey> map = new HashMap<>();
		map.put(key.computePubKey(), key);
		return new GlobalFsGatewayDriver(node, map, checkpointPosStrategy);
	}

	public static GlobalFsGatewayDriver create(GlobalFsNode node, KeyPair keys, CheckpointPosStrategy checkpointPosStrategy) {
		Map<PubKey, PrivKey> map = new HashMap<>();
		map.put(keys.getPubKey(), keys.getPrivKey());
		return new GlobalFsGatewayDriver(node, map, checkpointPosStrategy);
	}

	public static FsClient createFsAdapter(GlobalFsNode node, KeyPair keys, String fsName, CheckpointPosStrategy checkpointPosStrategy) {
		return create(node, keys, checkpointPosStrategy).createFsAdapter(RepoID.of(keys, fsName));
	}

	public static FsClient createFsAdapter(GlobalFsNode node, PrivKey key, String fsName, CheckpointPosStrategy checkpointPosStrategy) {
		KeyPair keys = key.computeKeys();
		return create(node, keys, checkpointPosStrategy).createFsAdapter(RepoID.of(keys, fsName));
	}

	@Override
	public Promise<SerialConsumer<ByteBuf>> upload(GlobalPath path, long offset) {
		PubKey pubKey = path.getOwner();
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			return Promise.ofException(UNKNOWN_KEY);
		}
		long normalizedOffset = offset == -1 ? 0 : offset;
		long[] size = {normalizedOffset};

		long[] toSkip = {0};
		SHA256Digest[] digest = new SHA256Digest[1];

		// all the below cutting logic is also present on the server
		// but we simply skip the prefix to not (potentially) send it over the network
		// and also getting the proper digest for checkpoints ofc

		return node.getMetadata(path)
				.thenCompose(signedMeta -> {
					if (signedMeta == null) {
						digest[0] = new SHA256Digest();
						return Promise.complete();
					}
					// TODO anton: check signature here
					GlobalFsMetadata meta = signedMeta.getData();
					long metaSize = meta.getSize();
					if (offset > metaSize) {
						return Promise.ofException(new StacklessException(GlobalFsGatewayDriver.class, "Trying to upload at offset greater than the file size"));
					}
					toSkip[0] = metaSize - offset;
					return node.download(path, metaSize, 0)
							.thenCompose(supplier -> supplier.toCollector(toList()))
							.thenCompose(frames -> {
								if (frames.size() != 1) {
									return Promise.ofException(new StacklessException(GlobalFsGatewayDriver.class, "No checkpoint at metadata size position!"));
								}
								// TODO anton: check signature here
								digest[0] = frames.get(0).getCheckpoint().getData().getDigest();
								return Promise.complete();
							});
				})
				.thenCompose($ ->
						node.upload(path, offset != -1 ? offset + toSkip[0] : offset)
								.thenApply(consumer -> {
									LocalPath localPath = path.toLocalPath();
									return consumer
											.apply(FrameSigner.create(localPath, normalizedOffset + toSkip[0], checkpointPosStrategy, privKey, digest[0]))
											.apply(SerialByteBufCutter.create(toSkip[0]))
											.peek(buf -> size[0] += buf.readRemaining())
											.withAcknowledgement(ack -> ack
													.thenCompose($2 -> {
														GlobalFsMetadata meta = GlobalFsMetadata.of(localPath, size[0], timeProvider.currentTimeMillis());
														return node.pushMetadata(pubKey, SignedData.sign(meta, privKey));
													}));
								}));
	}

	@Override
	public Promise<SerialSupplier<ByteBuf>> download(GlobalPath path, long offset, long limit) {
		return node.download(path, offset, limit)
				.thenApply(supplier -> supplier.apply(new FrameVerifier(path.toLocalPath(), path.getOwner(), offset, limit)));
	}

	@Override
	public Promise<List<GlobalFsMetadata>> list(RepoID space, String glob) {
		PubKey pubKey = space.getOwner();
		return node.list(space, glob)
				.thenApply(res -> res.stream()
						.filter(signedMeta -> signedMeta.verify(pubKey))
						.map(SignedData::getData)
						.collect(toList()));
	}

	@Override
	public Promise<Void> delete(GlobalPath path) {
		PubKey pubKey = path.getOwner();
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			return Promise.ofException(UNKNOWN_KEY);
		}
		return node.pushMetadata(pubKey, SignedData.sign(GlobalFsMetadata.ofRemoved(path.toLocalPath(), timeProvider.currentTimeMillis()), privKey));
	}

	@Override
	public Promise<Void> delete(RepoID space, String glob) {
		PubKey pubKey = space.getOwner();
		PrivKey privKey = keymap.get(pubKey);
		if (privKey == null) {
			return Promise.ofException(UNKNOWN_KEY);
		}
		if (!isWildcard(glob)) {
			return delete(GlobalPath.of(space, glob));
		}
		return node.list(space, glob)
				.thenCompose(list ->
						Promises.all(list.stream()
								.filter(signedMeta -> signedMeta.verify(pubKey))
								.map(signedMeta ->
										node.pushMetadata(pubKey, SignedData.sign(signedMeta.getData().toRemoved(), privKey)))));
	}

	@Override
	public FsClient createFsAdapter(RepoID repo, CurrentTimeProvider timeProvider) {
		if (!keymap.containsKey(repo.getOwner())) {
			throw new IllegalArgumentException("Cannot get remotefs adapter for space with unknown public key");
		}
		return GlobalFsGateway.super.createFsAdapter(repo, timeProvider);
	}
}
