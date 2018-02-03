package io.datakernel.logfs.ot;

import io.datakernel.logfs.ot.LogDiff.LogPositionDiff;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.TransformResult.ConflictResolution;
import io.datakernel.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static io.datakernel.aggregation.AggregationUtils.transformValuesToLinkedMap;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.CollectionUtils.intersection;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.singletonList;

public class LogOT {
	public static <T> OTSystem<LogDiff<T>> createLogOT(OTSystem<T> otSystem) {
		return OTSystemImpl.<LogDiff<T>>create()
				.withTransformFunction(LogDiff.class, LogDiff.class, (left, right) -> {
					Set<String> intersection = intersection(left.getPositions().keySet(), right.getPositions().keySet());
					if (intersection.isEmpty()) {
						TransformResult<T> transformed = otSystem.transform(left.getDiffs(), right.getDiffs());
						if (transformed.hasConflict()) {
							return TransformResult.conflict(transformed.resolution);
						}
						return TransformResult.of(
								LogDiff.of(right.getPositions(), transformed.left),
								LogDiff.of(left.getPositions(), transformed.right));
					}

					int comparison = 0;
					for (String log : intersection) {
						LogPositionDiff leftPosition = left.getPositions().get(log);
						LogPositionDiff rightPosition = right.getPositions().get(log);
						Preconditions.check(leftPosition.from.equals(rightPosition.from));
						comparison += leftPosition.compareTo(rightPosition);
					}

					return TransformResult.conflict(comparison > 0 ? ConflictResolution.LEFT : ConflictResolution.RIGHT);
				})
				.withEmptyPredicate(LogDiff.class, commit -> commit.getPositions().isEmpty() && commit.getDiffs().stream().allMatch(otSystem::isEmpty))
				.withSquashFunction(LogDiff.class, LogDiff.class, (commit1, commit2) -> {
					Map<String, LogPositionDiff> positions = new HashMap<>(commit1.getPositions());
					for (Entry<String, LogPositionDiff> entry : commit2.getPositions().entrySet()) {
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
					List<T> ops = concat(commit1.getDiffs(), commit2.getDiffs());
					return LogDiff.of(positions, otSystem.squash(ops));
				})
				.withInvertFunction(LogDiff.class, commit -> singletonList(LogDiff.of(
						transformValuesToLinkedMap(commit.getPositions().entrySet().stream(), LogPositionDiff::inverse),
						otSystem.invert(commit.getDiffs()))))
				;

	}

}
