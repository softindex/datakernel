package io.datakernel.logfs.ot;

import io.datakernel.logfs.ot.LogDiff.LogPositionDiff;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.TransformResult.ConflictResolution;
import io.datakernel.util.Preconditions;

import java.util.*;

import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.intersection;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.singletonList;

public class LogOT {
	public static <T> OTSystem<LogDiff<T>> createLogOT(final OTSystem<T> otSystem) {
		return OTSystemImpl.<LogDiff<T>>create()
				.withTransformFunction(LogDiff.class, LogDiff.class, (left, right) -> {
					Set<String> intersection = intersection(left.positions.keySet(), right.positions.keySet());
					if (intersection.isEmpty()) {
						TransformResult<T> transformed = otSystem.transform(left.diffs, right.diffs);
						if (transformed.hasConflict()) {
							return TransformResult.conflict(transformed.resolution);
						}
						return TransformResult.of(
								LogDiff.of(right.positions, transformed.left),
								LogDiff.of(left.positions, transformed.right));
					}

					int comparison = 0;
					for (String log : intersection) {
						LogPositionDiff leftPosition = left.positions.get(log);
						LogPositionDiff rightPosition = right.positions.get(log);
						Preconditions.check(leftPosition.from.equals(rightPosition.from));
						comparison += leftPosition.compareTo(rightPosition);
					}

					return TransformResult.conflict(comparison > 0 ? ConflictResolution.LEFT : ConflictResolution.RIGHT);
				})
				.withEmptyPredicate(LogDiff.class, commit -> commit.positions.isEmpty() && all(commit.diffs, otSystem::isEmpty))
				.withSquashFunction(LogDiff.class, LogDiff.class, (commit1, commit2) -> {
					Map<String, LogPositionDiff> positions = new HashMap<>(commit1.positions);
					for (Map.Entry<String, LogPositionDiff> entry : commit2.positions.entrySet()) {
						String log = entry.getKey();
						LogPositionDiff positionDiff1 = positions.get(log);
						LogPositionDiff positionDiff2 = entry.getValue();
						if (positionDiff1 != null) {
							checkState(positionDiff1.to.equals(positionDiff2.from));
							positionDiff2 = new LogPositionDiff(positionDiff1.from, positionDiff2.to);
						}
						if (!positionDiff2.isEmpty()) {
							positions.put(log, positionDiff2);
						} else {
							positions.remove(log);
						}
					}
					List<T> ops = new ArrayList<>(commit1.diffs.size() + commit2.diffs.size());
					ops.addAll(commit1.diffs);
					ops.addAll(commit2.diffs);
					return LogDiff.of(positions, otSystem.squash(ops));
				})
				.withInvertFunction(LogDiff.class, commit -> singletonList(LogDiff.of(
						transformValues(commit.positions, LogPositionDiff::inverse),
						otSystem.invert(commit.diffs))))
				;

	}

	private static <T> LogDiff<T> inverse(OTSystem<T> otSystem, LogDiff<T> commit) {
		return LogDiff.of(
				transformValues(commit.positions, LogPositionDiff::inverse),
				otSystem.invert(commit.diffs));
	}
}
