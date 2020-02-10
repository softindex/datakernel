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

import io.datakernel.common.exception.StacklessException;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

public interface DiscoveryService extends SharedKeyManager {
	StacklessException REJECTED_OUTDATED_ANNOUNCE_DATA = new StacklessException(DiscoveryService.class, "Rejected announce data as outdated");

	Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData);

	Promise<@Nullable SignedData<AnnounceData>> find(PubKey space);

	Promise<Map<PubKey, Set<RawServerId>>> findAll();
}
