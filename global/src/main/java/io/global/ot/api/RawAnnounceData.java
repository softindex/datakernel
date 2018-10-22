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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.ByteArrayIdentity;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.ot.util.BinaryDataFormats;

import java.util.*;

import static io.global.ot.util.BinaryDataFormats.sizeof;

public final class RawAnnounceData implements ByteArrayIdentity {
	private final byte[] bytes;

	private final long timestamp;
	private final PubKey pubKey;
	private final Set<RawServerId> serverIds;

	// region creators
	public RawAnnounceData(byte[] bytes, long timestamp, PubKey pubKey, Set<RawServerId> serverIds) {
		this.bytes = bytes;
		this.timestamp = timestamp;
		this.pubKey = pubKey;
		this.serverIds = serverIds;
	}

	public static RawAnnounceData of(long timestamp, PubKey pubKey, Set<RawServerId> serverIds) {
		List<RawServerId> ids = new ArrayList<>(serverIds);
		ByteBuf buf = ByteBufPool.allocate(8 + sizeof(pubKey) + sizeof(ids, BinaryDataFormats::sizeof));
		buf.writeLong(timestamp);
		BinaryDataFormats.writePubKey(buf, pubKey);
		BinaryDataFormats.writeCollection(buf, ids, BinaryDataFormats::writeRawServerId);
		return new RawAnnounceData(buf.asArray(), timestamp, pubKey, serverIds);
	}
	// endregion

	public static RawAnnounceData fromBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		long timestamp = buf.readLong();
		PubKey pubKey = BinaryDataFormats.readPubKey(buf);
		List<RawServerId> rawServerIds = BinaryDataFormats.readList(buf, BinaryDataFormats::readRawServerId);
		return new RawAnnounceData(bytes, timestamp, pubKey, new HashSet<>(rawServerIds));
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public Set<RawServerId> getServerIds() {
		return serverIds;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawAnnounceData that = (RawAnnounceData) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}

	@Override
	public String toString() {
		return "RawAnnounceData{timestamp=" + timestamp + ", pubKey=" + pubKey + ", serverIds=" + serverIds + '}';
	}
}
