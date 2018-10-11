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

package io.global.ot.api;

import io.datakernel.async.Stage;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;

import java.util.HashMap;

public class RawDiscoveryServiceStub implements RawDiscoveryService {
	private final HashMap<PubKey, SignedData<AnnounceData>> map = new HashMap<>();

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		return Stage.of(map.get(pubKey));
	}

	@Override
	public Stage<Void> announce(SignedData<AnnounceData> announceData) {
		map.compute(announceData.getData().getPubKey(),
				(pubKey, existing) ->
						announceData.getData().getTimestamp() > existing.getData().getTimestamp() ? announceData : existing);
		return Stage.complete();
	}
}
