package io.datakernel.etl;

import io.datakernel.multilog.LogPosition;

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

		if (from != null ? !from.equals(that.from) : that.from != null) return false;
		return to != null ? to.equals(that.to) : that.to == null;
	}

	@Override
	public int hashCode() {
		int result = from != null ? from.hashCode() : 0;
		result = 31 * result + (to != null ? to.hashCode() : 0);
		return result;
	}
}
