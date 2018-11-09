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

import io.datakernel.async.Promise;
import io.datakernel.exception.StacklessException;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;

import java.util.List;

public interface DiscoveryService {
	StacklessException NO_ANNOUNCE_DATA = new StacklessException(DiscoveryService.class, "Announce data not found");
	StacklessException NO_KEY = new StacklessException(DiscoveryService.class, "Key not found");

	Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData);

	Promise<SignedData<AnnounceData>> find(PubKey space);

	Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey);

	Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash);

	Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver);
}
