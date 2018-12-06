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

package io.datakernel.ot.counter.storage;

import io.datakernel.async.Promise;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyStorage;

import java.util.*;

public class InMemorySharedKeyStorage implements SharedKeyStorage {
	private final Map<PubKey, List<SignedData<SharedSimKey>>> keys = new HashMap<>();

	@Override
	public Promise<Void> store(PubKey receiver, SignedData<SharedSimKey> signedSharedSimKey) {
		keys.computeIfAbsent(receiver, $ -> new ArrayList<>()).add(signedSharedSimKey);
		return Promise.complete();
	}

	@Override
	public Promise<SignedData<SharedSimKey>> load(PubKey receiver, Hash hash) {
		return loadAll(receiver)
				.thenCompose(signedDataList -> {
					Optional<SignedData<SharedSimKey>> maybeKey = signedDataList.stream()
							.filter(signedData -> signedData.getValue().getHash().equals(hash))
							.findFirst();
					return maybeKey.isPresent() ? Promise.of(maybeKey.get()) : Promise.ofException(NO_SHARED_KEY);
				});
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> loadAll(PubKey receiver) {
		List<SignedData<SharedSimKey>> signedDataList = keys.get(receiver);
		if (signedDataList == null) {
			return Promise.ofException(NO_SHARED_KEY);
		}
		return Promise.of(signedDataList);
	}
}
