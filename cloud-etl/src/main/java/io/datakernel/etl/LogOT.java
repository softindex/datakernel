package io.datakernel.etl;

import io.datakernel.ot.TransformResult;
import io.datakernel.ot.TransformResult.ConflictResolution;
import io.datakernel.ot.system.OTSystem;
import io.datakernel.ot.system.OTSystemImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.common.collection.CollectionUtils.*;
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
						checkArgument(leftPosition.from.equals(rightPosition.from),
								"'From' values should be equal for left and right log positions");
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
							checkState(positionDiff1.to.equals(positionDiff2.from),
									"'To' value of the first log position should be equal to 'From' value of the second log position");
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
						transformMapValues(commit.getPositions(), LogPositionDiff::inverse),
						otSystem.invert(commit.getDiffs()))))
				;

	}

}
