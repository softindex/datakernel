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
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public final class GlobalFsDriver {
	private final Map<PubKey, PrivKey> keys = new HashMap<>();
	private final GlobalFsNode node;
	private final CheckpointPosStrategy checkpointPosStrategy;

	@Nullable
	private SimKey currentSimKey = null;

	private GlobalFsDriver(GlobalFsNode node, Map<PubKey, PrivKey> keys, CheckpointPosStrategy checkpointPosStrategy) {
		this.node = node;
		this.keys.putAll(keys);
		this.checkpointPosStrategy = checkpointPosStrategy;
	}

	public static GlobalFsDriver create(GlobalFsNode node, Map<PubKey, PrivKey> keys, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsDriver(node, keys, checkpointPosStrategy);
	}

	public static GlobalFsDriver create(GlobalFsNode node, List<KeyPair> keys, CheckpointPosStrategy checkpointPosStrategy) {
		return new GlobalFsDriver(node, keys.stream().collect(toMap(KeyPair::getPubKey, KeyPair::getPrivKey)), checkpointPosStrategy);
	}

	private PrivKey getPrivKey(PubKey pubKey) {
		PrivKey privKey = keys.get(pubKey);
		if (privKey == null) {
			throw new IllegalArgumentException("No private key stored for " + pubKey);
		}
		return privKey;
	}

	public FsClient createClientFor(PubKey pubKey) {
		return new GlobalFsGatewayAdapter(this, node, pubKey, getPrivKey(pubKey), checkpointPosStrategy);
	}

	@Nullable
	public SimKey getCurrentSimKey() {
		return currentSimKey;
	}

	public void setCurrentSimKey(@Nullable SimKey currentSimKey) {
		this.currentSimKey = currentSimKey;
	}
}
