package io.datakernel.etl;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.multilog.LogPosition;
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
		return new LogOTState<>(dataState);
	}

	public Map<String, LogPosition> getPositions() {
		return unmodifiableMap(positions);
	}

	public OTState<D> getDataState() {
		return dataState;
	}

	@Override
	public Promise<Void> init() {
		positions.clear();
		return dataState.init();
	}

	@Override
	public Promise<Void> apply(LogDiff<D> logDiff) {
		for (String log : logDiff.getPositions().keySet()) {
			LogPositionDiff positionDiff = logDiff.getPositions().get(log);
			positions.put(log, positionDiff.to);
		}
		return Promises.sequence(logDiff.getDiffs().stream().map(op -> () -> dataState.apply(op)));
	}

	@Override
	public String toString() {
		return "LogOTState{" +
				"positions=" + positions +
				", dataState=" + dataState +
				'}';
	}
}
