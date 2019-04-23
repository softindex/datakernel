package io.global.pn.api;

public final class RawMessage {
	private final long id;
	private final long timestamp;
	private final byte[] encrypted;

	public RawMessage(long id, long timestamp, byte[] encrypted) {
		this.id = id;
		this.timestamp = timestamp;
		this.encrypted = encrypted;
	}

	public long getId() {
		return id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public byte[] getEncrypted() {
		return encrypted;
	}
}
