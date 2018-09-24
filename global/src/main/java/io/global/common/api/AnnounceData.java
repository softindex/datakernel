package io.global.common.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.Signable;
import io.global.globalsync.util.SerializationUtils;

import java.io.IOException;
import java.util.*;

import static io.global.globalsync.util.SerializationUtils.sizeof;

public final class AnnounceData implements Signable {
	private final byte[] bytes;

	private final long timestamp;
	private final PubKey pubKey;
	private final Set<RawServerId> serverIds;

	public AnnounceData(byte[] bytes, long timestamp, PubKey pubKey, Set<RawServerId> serverIds) {
		this.bytes = bytes;
		this.timestamp = timestamp;
		this.pubKey = pubKey;
		this.serverIds = serverIds;
	}

	public static AnnounceData of(long timestamp, PubKey pubKey, Set<RawServerId> serverIds) {
		List<RawServerId> ids = new ArrayList<>(serverIds);
		ByteBuf buf = ByteBufPool.allocate(8 + sizeof(pubKey) + sizeof(ids, SerializationUtils::sizeof));
		buf.writeLong(timestamp);
		SerializationUtils.writePubKey(buf, pubKey);
		SerializationUtils.writeList(buf, ids, SerializationUtils::writeRawServerId);
		return new AnnounceData(buf.asArray(), timestamp, pubKey, serverIds);
	}

	public static AnnounceData fromBytes(byte[] bytes) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		long timestamp = buf.readLong();
		PubKey pubKey = SerializationUtils.readPubKey(buf);
		List<RawServerId> rawServerIds = SerializationUtils.readList(buf, SerializationUtils::readRawServerId);
		return new AnnounceData(bytes, timestamp, pubKey, new HashSet<>(rawServerIds));
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
		AnnounceData that = (AnnounceData) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}

	@Override
	public String toString() {
		return "AnnounceData{timestamp=" + timestamp + ", pubKey=" + pubKey + ", serverIds=" + serverIds + '}';
	}
}
