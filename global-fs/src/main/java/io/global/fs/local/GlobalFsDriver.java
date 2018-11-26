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

import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PrivateKeyStorage;
import io.global.common.PubKey;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;

import java.util.List;

import static java.util.stream.Collectors.toMap;

public final class GlobalFsDriver {
	private final GlobalFsNode node;
	private final PrivateKeyStorage privateKeyStorage;
	private final CheckpointPosStrategy checkpointPosStrategy;

	private GlobalFsDriver(GlobalFsNode node, PrivateKeyStorage privateKeyStorage, CheckpointPosStrategy checkpointPosStrategy) {
		this.node = node;
		this.checkpointPosStrategy = checkpointPosStrategy;
		this.privateKeyStorage = privateKeyStorage;
	}

	public static GlobalFsDriver create(GlobalFsNode node, PrivateKeyStorage privateKeyStorage, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsDriver(node, privateKeyStorage, checkpointPosStrategy);
	}

	public static GlobalFsDriver create(GlobalFsNode node, DiscoveryService discoveryService, List<KeyPair> keys, CheckpointPosStrategy checkpointPosStrategy) {
		PrivateKeyStorage pks = new PrivateKeyStorage(discoveryService, keys.stream().collect(toMap(KeyPair::getPubKey, KeyPair::getPrivKey)));
		return new GlobalFsDriver(node, pks, checkpointPosStrategy);
	}

	public FsClient gatewayFor(PubKey pubKey) {
		PrivKey privKey = privateKeyStorage.getManagedKey(pubKey);
		if (privKey == null) {
			throw new IllegalArgumentException("No private key stored for " + pubKey);
		}
		return new GlobalFsGateway(this, node, pubKey, privKey, checkpointPosStrategy);
	}

	public PrivateKeyStorage getPrivateKeyStorage() {
		return privateKeyStorage;
	}
}
