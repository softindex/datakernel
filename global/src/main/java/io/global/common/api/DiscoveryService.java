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

package io.global.common.api;

import io.datakernel.async.Stage;
import io.global.common.*;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface DiscoveryService {
	Stage<Void> announce(RepoID repo, SignedData<AnnounceData> announceData);

	Stage<Optional<SignedData<AnnounceData>>> find(RepoID repo);

	Stage<List<SignedData<AnnounceData>>> find(PubKey owner);

	default Stage<Void> announce(RepoID repo, AnnounceData announceData, PrivKey privKey) {
		return announce(repo, SignedData.sign(announceData, privKey));
	}

	default Stage<Void> append(RepoID repo, AnnounceData announceData, PrivKey privKey) {
		return find(repo)
				.thenCompose(data -> {
					if (!data.isPresent()) {
						return announce(repo, announceData, privKey);
					}
					Set<RawServerId> serverIds = new HashSet<>(data.get().getData().getServerIds());
					serverIds.addAll(announceData.getServerIds());
					long timestamp = Math.max(announceData.getTimestamp(), data.get().getData().getTimestamp());
					return announce(repo, AnnounceData.of(timestamp, serverIds), privKey);
				});
	}

	Stage<Void> shareKey(PubKey owner, SignedData<SharedSimKey> simKey);

	default Stage<Void> shareKey(KeyPair keys, SharedSimKey simKey) {
		return shareKey(keys.getPubKey(), SignedData.sign(simKey, keys.getPrivKey()));
	}

	Stage<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey owner, PubKey receiver, Hash hash);
}
