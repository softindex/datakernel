package io.global.pm.api;

import io.datakernel.common.parse.ParseException;
import io.global.common.PubKey;

import java.util.concurrent.ThreadLocalRandom;

public final class Message<T> {
	private final long id;
	private final long timestamp;
	private final PubKey sender;
	private final T payload;

	private Message(long id, long timestamp, PubKey sender, T payload) {
		this.id = id;
		this.timestamp = timestamp;
		this.sender = sender;
		this.payload = payload;
	}

	public static <T> Message<T> parse(long id, long timestamp, PubKey sender, T payload) throws ParseException {
		return new Message<>(id, timestamp, sender, payload);
	}

	public static <T> Message<T> of(long timestamp, PubKey sender, T payload) {
		return new Message<>(ThreadLocalRandom.current().nextLong(), timestamp, sender, payload);
	}

	public static <T> Message<T> now(PubKey sender, T payload) {
		return new Message<>(ThreadLocalRandom.current().nextLong(), System.currentTimeMillis(), sender, payload);
	}

	public long getId() {
		return id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public PubKey getSender() {
		return sender;
	}

	public T getPayload() {
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

		Message<?> message = (Message<?>) o;

		return id == message.id
				&& timestamp == message.timestamp
				&& sender.equals(message.sender)
				&& payload.equals(message.payload);

	}

	@Override
	public int hashCode() {
		return 29791 * (int) (id ^ (id >>> 32))
				+ 961 * (int) (timestamp ^ (timestamp >>> 32))
				+ 31 * sender.hashCode()
				+ payload.hashCode();
	}

	@Override
	public String toString() {
		return "Message{payload=" + payload + ", timestamp=" + timestamp + ", sender=" + sender + ", id=" + Long.toHexString(id) + '}';
	}
}
