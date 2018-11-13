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

import io.datakernel.exception.ParseException;
import io.global.common.RawServerId;

import java.util.Set;

public final class AnnounceData {
	private final long timestamp;
	private final Set<RawServerId> serverIds;

	// region creators
	public AnnounceData(long timestamp, Set<RawServerId> serverIds) {
		this.timestamp = timestamp;
		this.serverIds = serverIds;
	}

	public static AnnounceData of(long timestamp, Set<RawServerId> serverIds) {
		return new AnnounceData(timestamp, serverIds);
	}

	public static AnnounceData parse(long timestamp, Set<RawServerId> serverIds) throws ParseException {
		return new AnnounceData(timestamp, serverIds);
	}
	// endregion

	public long getTimestamp() {
		return timestamp;
	}

	public Set<RawServerId> getServerIds() {
		return serverIds;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AnnounceData that = (AnnounceData) o;
		return timestamp == that.timestamp && serverIds.equals(that.serverIds);
	}

	@Override
	public int hashCode() {
		return 31 * (int) (timestamp ^ (timestamp >>> 32)) + serverIds.hashCode();
	}

	@Override
	public String toString() {
		return "AnnounceData{timestamp=" + timestamp + ", serverIds=" + serverIds + '}';
	}
}
