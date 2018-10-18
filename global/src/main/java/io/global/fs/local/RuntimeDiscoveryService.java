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

import io.datakernel.async.Stage;
import io.datakernel.exception.StacklessException;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public final class RuntimeDiscoveryService implements DiscoveryService {
	private static final Logger logger = LoggerFactory.getLogger(RuntimeDiscoveryService.class);

	public static final StacklessException CANNOT_VERIFY_ANNOUNCE_DATA = new StacklessException(RuntimeDiscoveryService.class, "Cannot verify announce data");

	private final Map<PubKey, SignedData<AnnounceData>> announced = new HashMap<>();

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		return Stage.of(announced.get(pubKey));
	}

	@Override
	public Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData) {
		logger.info("Announcement: " + announceData);
		if (!announceData.verify(pubKey)) {
			logger.warn("Failed to verify: " + announceData);
			return Stage.ofException(CANNOT_VERIFY_ANNOUNCE_DATA);
		}
		announced.compute(pubKey, ($, existing) -> {
			if (existing != null && existing.getData().getTimestamp() > announceData.getData().getTimestamp()) {
				logger.info("Rejected as outdated: " + announceData);
				return existing;
			}
			return announceData;
		});
		return Stage.complete();
	}
}
