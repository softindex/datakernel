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
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class RuntimeDiscoveryService implements DiscoveryService {
	private static final Logger logger = LoggerFactory.getLogger(RuntimeDiscoveryService.class);

	public static final StacklessException REJECTED_OUTDATED_ANNOUNCE_DATA = new StacklessException(RuntimeDiscoveryService.class, "Rejected announce data as outdated");
	public static final StacklessException CANNOT_VERIFY_ANNOUNCE_DATA = new StacklessException(RuntimeDiscoveryService.class, "Cannot verify announce data");
	public static final StacklessException CANNOT_VERIFY_SHARED_KEY = new StacklessException(RuntimeDiscoveryService.class, "Cannot verify shared key");

	private final Map<PubKey, SignedData<AnnounceData>> announced = new HashMap<>();
	private final Map<ShareInfo, SignedData<SharedSimKey>> sharedKeys = new HashMap<>();

	@Override
	public Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData) {
		logger.info("received {} for {}", announceData, space);
		if (!announceData.verify(space)) {
			logger.warn("failed to verify: {}", announceData);
			return Promise.ofException(CANNOT_VERIFY_ANNOUNCE_DATA);
		}
		SignedData<AnnounceData> signedAnnounceData = announced.get(space);
		if (signedAnnounceData != null && signedAnnounceData.getData().getTimestamp() >= announceData.getData().getTimestamp()) {
			logger.info("rejected as outdated: {}", announceData);
			return Promise.ofException(REJECTED_OUTDATED_ANNOUNCE_DATA);
		}
		announced.put(space, announceData);
		return Promise.complete();
	}

	@Override
	public Promise<SignedData<AnnounceData>> find(PubKey space) {
		SignedData<AnnounceData> signedAnnounceData = announced.get(space);
		return signedAnnounceData != null ?
				Promise.of(signedAnnounceData) :
				Promise.ofException(NO_ANNOUNCE_DATA);
	}

	@Override
	public Promise<Void> shareKey(PubKey owner, SignedData<SharedSimKey> simKey) {
		logger.info("received {}", simKey);
		if (!simKey.verify(owner)) {
			logger.warn("failed to verify {}", simKey);
			return Promise.ofException(CANNOT_VERIFY_SHARED_KEY);
		}
		SharedSimKey data = simKey.getData();
		sharedKeys.put(new ShareInfo(owner, data.getReceiver(), data.getHash()), simKey);
		return Promise.complete();
	}

	@Override
	public Promise<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey owner, PubKey receiver, Hash hash) {
		return Promise.of(Optional.ofNullable(sharedKeys.get(new ShareInfo(owner, receiver, hash))));
	}

	private static class ShareInfo {
		final PubKey owner;
		final PubKey receiver;
		final Hash hash;

		public ShareInfo(PubKey owner, PubKey receiver, Hash hash) {
			this.owner = owner;
			this.receiver = receiver;
			this.hash = hash;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ShareInfo shareInfo = (ShareInfo) o;

			return owner.equals(shareInfo.owner) && receiver.equals(shareInfo.receiver) && hash.equals(shareInfo.hash);
		}

		@Override
		public int hashCode() {
			return 961 * owner.hashCode() +
					31 * receiver.hashCode() +
					hash.hashCode();
		}
	}
}
