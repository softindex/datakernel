package io.datakernel.etl;

import io.datakernel.multilog.LogPosition;

import java.util.Objects;

public final class LogPositionDiff implements Comparable<LogPositionDiff> {
	public final LogPosition from;
	public final LogPosition to;

	public LogPositionDiff(LogPosition from, LogPosition to) {
		this.from = from;
		this.to = to;
	}

	public LogPositionDiff inverse() {
		return new LogPositionDiff(to, from);
	}

	public boolean isEmpty() {
		return from.equals(to);
	}

	@Override
	public int compareTo(LogPositionDiff o) {
		return this.to.compareTo(o.to);
	}

	@Override
	public String toString() {
		return "LogPositionDiff{" +
				"from=" + from +
				", to=" + to +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LogPositionDiff that = (LogPositionDiff) o;

		if (!Objects.equals(from, that.from)) return false;
		return Objects.equals(to, that.to);
	}

	@Override
	public int hashCode() {
		int result = from != null ? from.hashCode() : 0;
		result = 31 * result + (to != null ? to.hashCode() : 0);
		return result;
	}
}
