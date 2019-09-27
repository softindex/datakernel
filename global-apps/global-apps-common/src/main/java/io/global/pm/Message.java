package io.global.pm;

import io.datakernel.exception.ParseException;
import io.global.common.PubKey;

public final class Message<K, V> {
	private final K id;
	private final long timestamp;
	private final PubKey sender;
	private final V payload;

	private Message(K id, long timestamp, PubKey sender, V payload) {
		this.id = id;
		this.timestamp = timestamp;
		this.sender = sender;
		this.payload = payload;
	}

	public static <K, V> Message<K, V> parse(K id, long timestamp, PubKey sender, V payload) throws ParseException {
		return new Message<>(id, timestamp, sender, payload);
	}

	public static <K, V> Message<K, V> of(K id, long timestamp, PubKey sender, V payload) {
		return new Message<>(id, timestamp, sender, payload);
	}

	public static <K, V> Message<K, V> now(K id, PubKey sender, V payload) {
		return new Message<>(id, System.currentTimeMillis(), sender, payload);
	}

	public K getId() {
		return id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public PubKey getSender() {
		return sender;
	}

	public V getPayload() {
		return payload;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Message<?, ?> message = (Message<?, ?>) o;

		return id.equals(message.id)
				&& timestamp == message.timestamp
				&& sender.equals(message.sender)
				&& payload.equals(message.payload);

	}

	@Override
	public int hashCode() {
		int result = id.hashCode();
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + sender.hashCode();
		result = 31 * result + payload.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Message{payload=" + payload + ", timestamp=" + timestamp + ", sender=" + sender + ", id=" + id + '}';
	}
}
