package io.global.pm.api;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public final class RawMessage {
	private final long id;
	private final long timestamp;
	@Nullable
	private final byte[] encrypted;

	private RawMessage(long id, long timestamp, @Nullable byte[] encrypted) {
		this.id = id;
		this.timestamp = timestamp;
		this.encrypted = encrypted;
	}

	public static RawMessage of(long id, long timestamp, @NotNull byte[] encrypted) {
		return new RawMessage(id, timestamp, encrypted);
	}

	public static RawMessage parse(long id, long timestamp, @Nullable byte[] encrypted) {
		return new RawMessage(id, timestamp, encrypted);
	}

	public static RawMessage tombstone(long id, long timestamp) {
		return new RawMessage(id, timestamp, null);
	}

	public long getId() {
		return id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public byte[] getEncrypted() {
		assert encrypted != null;
		return encrypted;
	}

	public boolean isTombstone() {
		return encrypted == null;
	}

	public boolean isMessage() {
		return encrypted != null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RawMessage that = (RawMessage) o;

		if (id != that.id) return false;
		if (timestamp != that.timestamp) return false;
		if (!Arrays.equals(encrypted, that.encrypted)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (id ^ (id >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + Arrays.hashCode(encrypted);
		return result;
	}
}
