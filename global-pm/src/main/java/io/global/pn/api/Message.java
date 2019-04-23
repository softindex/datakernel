package io.global.pn.api;

import io.datakernel.exception.ParseException;
import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadLocalRandom;

public final class Message<T> {
	private final long id;
	private final long timestamp;
	private final PubKey sender;
	private final T payload;

	private Message(long id, long timestamp, PubKey sender, @Nullable T payload) {
		this.id = id;
		this.timestamp = timestamp;
		this.sender = sender;
		this.payload = payload;
	}

	public static <T> Message<T> parse(long id, long timestamp, PubKey sender, @Nullable T payload) throws ParseException {
		return new Message<>(id, timestamp, sender, payload);
	}

	public static <T> Message<T> of(long timestamp, PubKey sender, @NotNull T payload) {
		return new Message<>(ThreadLocalRandom.current().nextLong(), timestamp, sender, payload);
	}

	public static <T> Message<T> now(PubKey sender, @NotNull T payload) {
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
	public String toString() {
		return "Message{payload=" + payload + ", timestamp=" + timestamp + ", sender=" + sender + ", id=" + Long.toHexString(id) + '}';
	}
}
