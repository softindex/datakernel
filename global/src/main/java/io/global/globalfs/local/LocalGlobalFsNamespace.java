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

package io.global.globalfs.local;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.AsyncSuppliers;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsNode;

import java.util.*;

final class LocalGlobalFsNamespace {
	private final Map<String, RemoteFsFileSystem> fileSystems = new HashMap<>();

	private final LocalGlobalFsNode node;
	private final PubKey pubKey;

	private final Map<RawServerId, GlobalFsNode> knownNodes = new HashMap<>();

	private final AsyncSupplier<List<GlobalFsNode>> findNodesImpl;

	private final AsyncSupplier<Void> fetch = AsyncSuppliers.reuse(() ->
			Stages.all(fileSystems.values().stream()
					.map(RemoteFsFileSystem::fetch)
					.map(Stage::toTry)));
	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();
	private long nodeDiscoveryTimestamp;

	// region creators
	public LocalGlobalFsNamespace(LocalGlobalFsNode node, PubKey pubKey) {
		this.node = node;
		this.pubKey = pubKey;

		knownNodes.put(node.getId(), node);

		findNodesImpl = AsyncSuppliers.reuse(() -> node.getDiscoveryService().findServers(pubKey))
				.transform(announceData -> {
					Set<RawServerId> newServerIds = announceData.getData().getServerIds();
					knownNodes.keySet().removeIf(t -> !newServerIds.contains(t));
					newServerIds.forEach(id -> knownNodes.computeIfAbsent(id, node.getClientFactory()::create));
					nodeDiscoveryTimestamp = now.currentTimeMillis();
					return new ArrayList<>(knownNodes.values());
				});
	}
	// endregion

	public LocalGlobalFsNode getNode() {
		return node;
	}

	public PubKey getKey() {
		return pubKey;
	}

	public RemoteFsFileSystem getFs(String fsName) {
		return fileSystems.computeIfAbsent(fsName, $ -> node.getFileSystemFactory().create(this, fsName));
	}

	private boolean areDiscoveredNodesValid() {
		return nodeDiscoveryTimestamp >= now.currentTimeMillis() - node.getSettings().getLatencyMargin().toMillis();
	}

	public Stage<List<GlobalFsNode>> findNodes() {
		if (areDiscoveredNodesValid()) {
			return Stage.of(new ArrayList<>(knownNodes.values()));
		}
		return findNodesImpl.get();
	}

	public Stage<Void> fetch() {
		return fetch.get();
	}
}
