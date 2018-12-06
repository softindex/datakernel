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
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.global.common.BinaryDataFormats.REGISTRY;

public class InMemoryAnnouncementStorage implements AnnouncementStorage {
	private final Map<PubKey, SignedData<AnnounceData>> announcements = new HashMap<>();
	private static final StructuredCodec<SignedData<AnnounceData>> SIGNED_ANNOUNCE = REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});

	@Override
	public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
		announcements.put(space, announceData);
		return Promise.complete();
	}

	@Override
	public Promise<SignedData<AnnounceData>> load(PubKey space) {
		SignedData<AnnounceData> signedData = announcements.get(space);
		if (signedData == null) {
			return Promise.ofException(NO_ANNOUNCEMENT);
		}
		try {
			return Promise.of(decode(SIGNED_ANNOUNCE, encode(SIGNED_ANNOUNCE, signedData)));
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}
}
