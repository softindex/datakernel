package io.datakernel.cube.ot;

import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationOT;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.union;
import static java.util.Collections.singletonList;

public class CubeOT {
	public static OTSystem<CubeDiff> createCubeOT() {
		OTSystem<AggregationDiff> aggregationOTSystem = AggregationOT.createAggregationOT();
		return OTSystemImpl.<CubeDiff>create()
				.withTransformFunction(CubeDiff.class, CubeDiff.class, (left, right) -> {
					Map<String, AggregationDiff> newOpsLeft = new LinkedHashMap<>();
					Map<String, AggregationDiff> newOpsRight = new LinkedHashMap<>();

					for (String aggregation : union(left.keySet(), right.keySet())) {
						AggregationDiff leftOps = left.get(aggregation);
						AggregationDiff rightOps = right.get(aggregation);
						if (leftOps == null) leftOps = AggregationDiff.empty();
						if (rightOps == null) rightOps = AggregationDiff.empty();

						TransformResult<AggregationDiff> transformed = aggregationOTSystem.transform(leftOps, rightOps);
						if (transformed.hasConflict())
							return TransformResult.conflict(transformed.resolution);

						if (transformed.left.size() > 1)
							throw new OTTransformException("Left transformation result has more than one aggregation diff");
						if (transformed.right.size() > 1)
							throw new OTTransformException("Right transformation result has more than one aggregation diff");

						if (!transformed.left.isEmpty())
							newOpsLeft.put(aggregation, transformed.left.get(0));
						if (!transformed.right.isEmpty())
							newOpsRight.put(aggregation, transformed.right.get(0));
					}
					return TransformResult.of(CubeDiff.of(newOpsLeft), CubeDiff.of(newOpsRight));
				})
				.withEmptyPredicate(CubeDiff.class, CubeDiff::isEmpty)
				.withInvertFunction(CubeDiff.class, commit -> singletonList(commit.inverse()))
				.withSquashFunction(CubeDiff.class, CubeDiff.class, (commit1, commit2) -> {
					Map<String, AggregationDiff> newOps = new LinkedHashMap<>();
					for (String aggregation : union(commit1.keySet(), commit2.keySet())) {
						AggregationDiff ops1 = commit1.get(aggregation);
						AggregationDiff ops2 = commit2.get(aggregation);
						if (ops1 == null) {
							newOps.put(aggregation, ops2);
						} else if (ops2 == null) {
							newOps.put(aggregation, ops1);
						} else {
							List<AggregationDiff> ops = new ArrayList<>();
							ops.add(ops1);
							ops.add(ops2);
							List<AggregationDiff> simplified = aggregationOTSystem.squash(ops);
							if (!simplified.isEmpty()) {
								if (simplified.size() > 1)
									throw new AssertionError();
								newOps.put(aggregation, simplified.get(0));
							}
						}
					}
					return CubeDiff.of(newOps);
				});
	}

}
