/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.global.common;

import io.datakernel.async.Promise;
import io.global.common.api.DiscoveryService;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.CryptoException;

import java.util.HashMap;
import java.util.Map;

import static io.global.common.api.SharedKeyStorage.NO_SHARED_KEY;

public final class PrivateKeyStorage {
	private final Map<Hash, SimKey> keyMap = new HashMap<>();
	private final Map<PubKey, PrivKey> keys;

	private final DiscoveryService discoveryService;

	@Nullable
	private SimKey currentSimKey;

	public PrivateKeyStorage(@Nullable DiscoveryService discoveryService, Map<PubKey, PrivKey> keys) {
		this.discoveryService = discoveryService;
		this.keys = new HashMap<>(keys);
	}

	public PrivateKeyStorage(Map<PubKey, PrivKey> keys) {
		this(null, keys);
	}

	public Map<PubKey, PrivKey> getKeys() {
		return keys;
	}

	@Nullable
	public PrivKey getManagedKey(PubKey pubKey) {
		return keys.get(pubKey);
	}

	public Promise<SimKey> getKey(PubKey receiver, @Nullable Hash simKeyHash) {
		if (simKeyHash == null) {
			return Promise.of(null);
		}
		SimKey key = keyMap.get(simKeyHash);
		if (key != null) {
			return Promise.of(key);
		}
		if (discoveryService == null) {
			return Promise.of(null);
		}
		return discoveryService.getSharedKey(receiver, simKeyHash)
				.thenCompose(signedSharedSimKey -> {
					SharedSimKey sharedSimKey = signedSharedSimKey.getValue();
					PrivKey privKey = keys.get(receiver);
					if (privKey == null) {
						return Promise.ofException(NO_SHARED_KEY);
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

	public void forget(SimKey simKey) {
		keyMap.remove(Hash.sha1(simKey.getBytes()));
	}

	public void forget(Hash simKeyHash) {
		keyMap.remove(simKeyHash);
	}
}
