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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FsClient;
import io.global.common.*;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import org.spongycastle.crypto.CryptoException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public final class GlobalFsDriver {
	private final Map<Hash, SimKey> keyMap = new HashMap<>();
	private final Map<PubKey, PrivKey> keys = new HashMap<>();
	private final GlobalFsNode node;
	private final DiscoveryService discoveryService;
	private final CheckpointPosStrategy checkpointPosStrategy;

	@Nullable
	private SimKey currentSimKey;

	private GlobalFsDriver(GlobalFsNode node, DiscoveryService discoveryService, Map<PubKey, PrivKey> keys, CheckpointPosStrategy checkpointPosStrategy) {
		this.node = node;
		this.discoveryService = discoveryService;
		this.keys.putAll(keys);
		this.checkpointPosStrategy = checkpointPosStrategy;

		// changeCurrentSimKey(SimKey.fromString("GBKadnXVViYZEYopiY0_sg")); // testing stub
	}

	public static GlobalFsDriver create(GlobalFsNode node, DiscoveryService discoveryService, Map<PubKey, PrivKey> keys, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsDriver(node, discoveryService, keys, checkpointPosStrategy);
	}

	public static GlobalFsDriver create(GlobalFsNode node, DiscoveryService discoveryService, List<KeyPair> keys, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsDriver(node, discoveryService, keys.stream().collect(toMap(KeyPair::getPubKey, KeyPair::getPrivKey)), checkpointPosStrategy);
	}

	public Promise<SimKey> getKey(PubKey receiver, @Nullable Hash simKeyHash) {
		if (simKeyHash == null) {
			return Promise.of(null);
		}
		SimKey key = keyMap.get(simKeyHash);
		if (key != null) {
			return Promise.of(key);
		}
		return discoveryService.getSharedKey(receiver, simKeyHash)
				.thenCompose(signedSharedSimKey -> {
					SharedSimKey sharedSimKey = signedSharedSimKey.getValue();
					PrivKey privKey = keys.get(receiver);
					if (privKey == null) {
						return Promise.ofException(new StacklessException(GlobalFsDriver.class, "No private key stored for " + receiver));
					}
					try {
						SimKey newKey = sharedSimKey.decryptSimKey(privKey);
						keyMap.put(sharedSimKey.getHash(), newKey);
						return Promise.of(newKey);
					} catch (CryptoException e) {
						return Promise.ofException(e);
					}
				});
	}

	public FsClient createClientFor(PubKey pubKey) {
		PrivKey privKey = keys.get(pubKey);
		if (privKey == null) {
			throw new IllegalArgumentException("No private key stored for " + pubKey);
		}
		return new GlobalFsGatewayAdapter(this, node, pubKey, privKey, checkpointPosStrategy);
	}

	@Nullable
	public SimKey getCurrentSimKey() {
		return currentSimKey;
	}

	public void changeCurrentSimKey(@Nullable SimKey currentSimKey) {
		this.currentSimKey = currentSimKey;
		if (currentSimKey != null) {
			keyMap.put(Hash.sha1(currentSimKey.getBytes()), currentSimKey);
		}
	}

	public void forget(Hash simKeyHash) {
		keyMap.remove(simKeyHash);
	}
}
