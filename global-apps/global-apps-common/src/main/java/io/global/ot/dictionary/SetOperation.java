package io.global.ot.dictionary;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class SetOperation {
	@Nullable
	private final String prev;
	@Nullable
	private final String next;

	public SetOperation(@Nullable String prev, @Nullable String next) {
		this.prev = prev;
		this.next = next;
	}

	public static SetOperation set(@Nullable String prevValue, @Nullable String nextValue) {
		return new SetOperation(prevValue, nextValue);
	}

	@Nullable
	public String getPrev() {
		return prev;
	}

	@Nullable
	public String getNext() {
		return next;
	}

	public SetOperation invert() {
		return new SetOperation(next, prev);
	}

	public boolean isEmpty() {
		return Objects.equals(prev, next);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SetOperation that = (SetOperation) o;

		if (prev != null ? !prev.equals(that.prev) : that.prev != null) return false;
		if (next != null ? !next.equals(that.next) : that.next != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = prev != null ? prev.hashCode() : 0;
		result = 31 * result + (next != null ? next.hashCode() : 0);
		return result;
	}
}
