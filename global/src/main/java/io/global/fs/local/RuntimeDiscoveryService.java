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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

public final class RuntimeDiscoveryService implements DiscoveryService {
	private static final Logger logger = LoggerFactory.getLogger(RuntimeDiscoveryService.class);

	public static final StacklessException REJECTED_OUTDATED_ANNOUNCE_DATA = new StacklessException(RuntimeDiscoveryService.class, "Rejected announce data as outdated");
	public static final StacklessException CANNOT_VERIFY_ANNOUNCE_DATA = new StacklessException(RuntimeDiscoveryService.class, "Cannot verify announce data");
	public static final StacklessException CANNOT_VERIFY_SHARED_KEY = new StacklessException(RuntimeDiscoveryService.class, "Cannot verify shared key");

	private final Map<PubKey, SignedData<AnnounceData>> announced = new HashMap<>();
	private final Map<PubKey, Map<Hash, SignedData<SharedSimKey>>> sharedKeys = new HashMap<>();

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
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		logger.info("received {}", simKey);
		sharedKeys.computeIfAbsent(receiver, $ -> new HashMap<>()).put(simKey.getData().getHash(), simKey);
		return Promise.complete();
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		Map<Hash, SignedData<SharedSimKey>> keys = sharedKeys.get(receiver);
		if (keys != null) {
			SignedData<SharedSimKey> data = keys.get(hash);
			if (data != null) {
				return Promise.of(data);
			}
		}
		return Promise.ofException(NO_KEY);
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return Promise.of(new ArrayList<>(sharedKeys.getOrDefault(receiver, emptyMap()).values()));
	}
}
