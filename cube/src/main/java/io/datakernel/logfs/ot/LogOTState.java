package io.datakernel.logfs.ot;

import io.datakernel.logfs.LogPosition;
import io.datakernel.ot.OTState;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public final class LogOTState<D> implements OTState<LogDiff<D>> {
	private final Map<String, LogPosition> positions = new HashMap<>();
	private final OTState<D> dataState;

	private LogOTState(OTState<D> dataState) {
		this.dataState = dataState;
	}

	public static <D> LogOTState<D> create(OTState<D> dataState) {
		return new LogOTState<D>(dataState);
	}

	public Map<String, LogPosition> getPositions() {
		return unmodifiableMap(positions);
	}

	public OTState<D> getDataState() {
		return dataState;
	}

	@Override
	public void init() {
		positions.clear();
		dataState.init();
	}

	@Override
	public void apply(LogDiff<D> op) {
		for (String log : op.positions.keySet()) {
			LogDiff.LogPositionDiff positionDiff = op.positions.get(log);
			positions.put(log, positionDiff.to);
		}
		for (D d : op.diffs) {
			dataState.apply(d);
		}
	}

	@Override
	public String toString() {
		return "LogOTState{" +
				"positions=" + positions +
				", dataState=" + dataState +
				'}';
	}
}
