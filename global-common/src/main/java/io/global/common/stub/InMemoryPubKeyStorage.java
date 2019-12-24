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

package io.global.common.stub;

import io.datakernel.promise.Promise;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.PubKeyStorage;
import io.global.common.api.SharedKeyStorage;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static java.util.Collections.emptyList;

public class InMemoryPubKeyStorage implements PubKeyStorage {
	private final Set<PubKey> keys = new HashSet<>();

	@Override
	public Promise<Set<PubKey>> loadPublicKeys() {
		return Promise.of(keys);
	}

	@Override
	public Promise<Void> storePublicKey(PubKey pubKey) {
		keys.add(pubKey);
		return Promise.complete();
	}

	@Override
	public Promise<Void> removePublicKey(PubKey pubKey) {
		keys.remove(pubKey);
		return Promise.complete();
	}

	public void clear() {
		keys.clear();
	}
}
