package io.datakernel.logfs.ot;

import io.datakernel.logfs.LogPosition;
import io.datakernel.ot.OTState;

import java.util.HashMap;
import java.util.Map;

public class LogOTState<D> implements OTState<LogDiff<D>> {
	public final Map<String, LogPosition> positions = new HashMap<>();
	public final OTState<D> dataState;

	public LogOTState(OTState<D> dataState) {
		this.dataState = dataState;
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
