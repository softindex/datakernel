package io.datakernel.logfs.ot;

import io.datakernel.logfs.LogPosition;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
		return new LogDiff<>(Collections.<String, LogPositionDiff>emptyMap(), diffs);
	}

	public static <D> LogDiff<D> forCurrentPosition(D diff) {
		return forCurrentPosition(singletonList(diff));
	}

	@Override
	public String toString() {
		return "LogDiff{" +
				"positions=" + positions +
				", diffs=" + diffs +
				'}';
	}
}
