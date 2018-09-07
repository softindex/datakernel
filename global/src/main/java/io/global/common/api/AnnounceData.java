package io.global.common.api;

import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.Signable;

import java.util.Arrays;
import java.util.Set;

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

	public static AnnounceData fromBytes(byte[] bytes) {
		throw new UnsupportedOperationException("stub");
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
}
