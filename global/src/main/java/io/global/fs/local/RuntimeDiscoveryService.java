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
import io.datakernel.exception.StacklessException;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class RuntimeDiscoveryService implements DiscoveryService {
	private static final Logger logger = LoggerFactory.getLogger(RuntimeDiscoveryService.class);

	public static final StacklessException CANNOT_VERIFY_ANNOUNCE_DATA = new StacklessException(RuntimeDiscoveryService.class, "Cannot verify announce data");
	public static final StacklessException CANNOT_VERIFY_SHARED_KEY = new StacklessException(RuntimeDiscoveryService.class, "Cannot verify shared key");

	private final Map<PubKey, Map<String, SignedData<AnnounceData>>> announced = new HashMap<>();
	private final Map<SimKeyKey, SignedData<SharedSimKey>> sharedKeys = new HashMap<>();

	@Override
	public Promise<Void> announce(RepoID repo, SignedData<AnnounceData> announceData) {
		logger.info("received {} for {}", announceData, repo);
		if (!announceData.verify(repo.getOwner())) {
			logger.warn("failed to verify " + announceData);
			return Promise.ofException(CANNOT_VERIFY_ANNOUNCE_DATA);
		}
		announced
				.computeIfAbsent(repo.getOwner(), $ -> new HashMap<>())
				.compute(repo.getName(), ($, existing) -> {
					if (existing != null && existing.getData().getTimestamp() > announceData.getData().getTimestamp()) {
						logger.info("rejected as outdated " + announceData);
						return existing;
					}
					return announceData;
				});
		return Promise.complete();
	}

	@Override
	public Promise<Optional<SignedData<AnnounceData>>> find(RepoID repo) {
		Map<String, SignedData<AnnounceData>> repos = announced.get(repo.getOwner());
		return repos != null ?
				Promise.of(Optional.ofNullable(repos.get(repo.getName()))) :
				Promise.of(Optional.empty());
	}

	@Override
	public Promise<List<SignedData<AnnounceData>>> find(PubKey owner) {
		return Promise.of(new ArrayList<>(announced.computeIfAbsent(owner, $ -> new HashMap<>()).values()));
	}

	@Override
	public Promise<Void> shareKey(PubKey owner, SignedData<SharedSimKey> simKey) {
		logger.info("received {}", simKey);
		if (!simKey.verify(owner)) {
			logger.warn("failed to verify " + simKey);
			return Promise.ofException(CANNOT_VERIFY_SHARED_KEY);
		}
		SharedSimKey data = simKey.getData();
		sharedKeys.put(new SimKeyKey(owner, data.getReceiver(), data.getHash()), simKey);
		return Promise.complete();
	}

	@Override
	public Promise<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey owner, PubKey receiver, Hash hash) {
		return Promise.of(Optional.ofNullable(sharedKeys.get(new SimKeyKey(owner, receiver, hash))));
	}

	private static class SimKeyKey {
		final PubKey owner;
		final PubKey receiver;
		final Hash hash;

		public SimKeyKey(PubKey owner, PubKey receiver, Hash hash) {
			this.owner = owner;
			this.receiver = receiver;
			this.hash = hash;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			SimKeyKey simKeyKey = (SimKeyKey) o;

			return owner.equals(simKeyKey.owner) && receiver.equals(simKeyKey.receiver) && hash.equals(simKeyKey.hash);
		}

		@Override
		public int hashCode() {
			return 31 * (31 * owner.hashCode() + receiver.hashCode()) + hash.hashCode();
		}
	}
}
