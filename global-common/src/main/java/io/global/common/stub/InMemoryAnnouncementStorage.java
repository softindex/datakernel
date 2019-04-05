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

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;

import java.util.HashMap;
import java.util.Map;

public class InMemoryAnnouncementStorage implements AnnouncementStorage {
	private final Map<PubKey, SignedData<AnnounceData>> announcements = new HashMap<>();

	@Override
	public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
		announcements.put(space, announceData);
		return Promise.complete();
	}

	@Override
	public Promise<SignedData<AnnounceData>> load(PubKey space) {
		return Promise.of(announcements.get(space));
	}

	public void addAnnouncements(Map<PubKey, SignedData<AnnounceData>> announcements) {
		this.announcements.putAll(announcements);
	}

	public void clear() {
		announcements.clear();
	}
}
