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

package io.global.db;

import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PrivateKeyStorage;
import io.global.common.PubKey;
import io.global.common.api.DiscoveryService;
import io.global.db.api.DbClient;
import io.global.db.api.GlobalDbNode;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public final class GlobalDbDriver {
	private final GlobalDbNode node;

	private final PrivateKeyStorage privateKeyStorage;

	private GlobalDbDriver(GlobalDbNode node, PrivateKeyStorage privateKeyStorage) {
		this.node = node;
		this.privateKeyStorage = privateKeyStorage;
	}

	public static GlobalDbDriver create(GlobalDbNode node, DiscoveryService discoveryService, Map<PubKey, PrivKey> keys) {
		return new GlobalDbDriver(node, new PrivateKeyStorage(discoveryService, keys));
	}

	public static GlobalDbDriver create(GlobalDbNode node, DiscoveryService discoveryService, List<KeyPair> keys) {
		return new GlobalDbDriver(node, new PrivateKeyStorage(discoveryService, keys.stream().collect(toMap(KeyPair::getPubKey, KeyPair::getPrivKey))));
	}

	public static GlobalDbDriver create(GlobalDbNode node, PrivateKeyStorage pks) {
		return new GlobalDbDriver(node, pks);
	}

	public DbClient gatewayFor(PubKey owner) {
		PrivKey privKey = privateKeyStorage.getManagedKey(owner);
		if (privKey == null) {
			throw new IllegalArgumentException("No private key stored for " + owner);
		}
		return new GlobalDbGateway(this, node, owner, privKey);
	}

	public PrivateKeyStorage getPrivateKeyStorage() {
		return privateKeyStorage;
	}
}
