package io.datakernel.logfs.ot;

import io.datakernel.logfs.LogPosition;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

public class LogDiff<D> {
	public static final class LogPositionDiff implements Comparable<LogPositionDiff> {
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

	public final Map<String, LogPositionDiff> positions;
	public final List<D> diffs;

	private LogDiff(Map<String, LogPositionDiff> positions, List<D> diffs) {
		this.positions = positions;
		this.diffs = diffs;
	}

	public static <D> LogDiff<D> of(Map<String, LogPositionDiff> positions, List<D> diffs) {
		return new LogDiff<>(positions, diffs);
	}

	public static <D> LogDiff<D> of(Map<String, LogPositionDiff> positions, D diff) {
		return new LogDiff<>(positions, singletonList(diff));
	}

	public static <D> LogDiff<D> forCurrentPosition(List<D> diffs) {
		return new LogDiff<>(Collections.emptyMap(), diffs);
	}

	public static <D> LogDiff<D> forCurrentPosition(D diff) {
		return forCurrentPosition(singletonList(diff));
	}

	public Stream<D> diffs() {
		return diffs.stream();
	}

	@Override
	public String toString() {
		return "LogDiff{" +
				"positions=" + positions +
				", diffs=" + diffs.size() +
				'}';
	}

	public String deepToString() {
		return "LogDiff{" +
				"positions=" + positions +
				", diffs=" + diffs +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LogDiff<?> logDiff = (LogDiff<?>) o;

		if (positions != null ? !positions.equals(logDiff.positions) : logDiff.positions != null) return false;
		return diffs != null ? diffs.equals(logDiff.diffs) : logDiff.diffs == null;
	}

	@Override
	public int hashCode() {
		int result = positions != null ? positions.hashCode() : 0;
		result = 31 * result + (diffs != null ? diffs.hashCode() : 0);
		return result;
	}
}
