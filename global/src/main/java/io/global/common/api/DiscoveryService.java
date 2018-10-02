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

package io.global.common.api;

import io.datakernel.async.Stage;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;

import java.util.HashSet;
import java.util.Set;

public interface DiscoveryService {
	Stage<SignedData<AnnounceData>> findServers(PubKey pubKey);

	Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData);

	default Stage<Void> announce(KeyPair keys, AnnounceData announceData) {
		return announce(keys.getPubKey(), SignedData.sign(announceData, keys.getPrivKey()));
	}

	default Stage<Void> append(KeyPair keys, AnnounceData announceData) {
		return findServers(keys.getPubKey())
				.thenCompose(data -> {
					if (data == null) {
						return announce(keys, announceData);
					}
					Set<RawServerId> serverIds = new HashSet<>(data.getData().getServerIds());
					serverIds.addAll(announceData.getServerIds());
					long timestamp = Math.max(announceData.getTimestamp(), data.getData().getTimestamp());
					return announce(keys, AnnounceData.of(timestamp, keys.getPubKey(), serverIds));
				});
	}
}
