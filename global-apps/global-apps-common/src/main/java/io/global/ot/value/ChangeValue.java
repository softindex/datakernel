package io.global.ot.value;

import java.util.Objects;

public final class ChangeValue<T> {
	private final T prev;
	private final T next;
	private final long timestamp;

	private ChangeValue(T prev, T next, long timestamp) {
		this.prev = prev;
		this.next = next;
		this.timestamp = timestamp;
	}

	public static <T> ChangeValue<T> of(T prev, T next, long timestamp) {
		return new ChangeValue<>(prev, next, timestamp);
	}

	public T getPrev() {
		return prev;
	}

	public T getNext() {
		return next;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isEmpty() {
		return next.equals(prev);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ChangeValue<?> value = (ChangeValue<?>) o;

		return timestamp == value.timestamp && Objects.equals(prev, value.prev) && Objects.equals(next, value.next);
	}

	@Override
	public int hashCode() {
		return 961 * (prev != null ? prev.hashCode() : 0) + 31 * (next != null ? next.hashCode() : 0) + (int) (timestamp ^ (timestamp >>> 32));
	}

	@Override
	public String toString() {
		return "ChangeValue{prev='" + prev + "', next='" + next + ", timestamp=" + timestamp + '}';
	}
}
