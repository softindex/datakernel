package io.global.ot.map;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class SetValue<T> {
	@Nullable
	private final T prev;
	@Nullable
	private final T next;

	private SetValue(@Nullable T prev, @Nullable T next) {
		this.prev = prev;
		this.next = next;
	}

	public static <T> SetValue<T> set(@Nullable T prevValue, @Nullable T nextValue) {
		return new SetValue<>(prevValue, nextValue);
	}

	@Nullable
	public T getPrev() {
		return prev;
	}

	@Nullable
	public T getNext() {
		return next;
	}

	public SetValue<T> invert() {
		return new SetValue<>(next, prev);
	}

	public boolean isEmpty() {
		return Objects.equals(prev, next);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SetValue that = (SetValue) o;

		return Objects.equals(prev, that.prev) && Objects.equals(next, that.next);
	}

	@Override
	public int hashCode() {
		int result = prev != null ? prev.hashCode() : 0;
		result = 31 * result + (next != null ? next.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "SetValue{" +
				"prev=" + prev +
				", next=" + next +
				'}';
	}
}
