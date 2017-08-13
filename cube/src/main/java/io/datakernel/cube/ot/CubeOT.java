package io.datakernel.cube.ot;

import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationOT;
import io.datakernel.ot.DiffPair;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Sets.union;
import static java.util.Collections.singletonList;

public class CubeOT {
	public static OTSystem<CubeDiff> createCubeOT() {
		final OTSystem<AggregationDiff> aggregationOTSystem = AggregationOT.createAggregationOT();
		return OTSystemImpl.<CubeDiff>create()
				.withTransformFunction(CubeDiff.class, CubeDiff.class, new OTSystemImpl.TransformFunction<CubeDiff, CubeDiff, CubeDiff>() {
					@Override
					public DiffPair<? extends CubeDiff> transform(CubeDiff left, CubeDiff right) {
						Map<String, AggregationDiff> newOpsLeft = new LinkedHashMap<>();
						Map<String, AggregationDiff> newOpsRight = new LinkedHashMap<>();

						for (String aggregation : union(left.keySet(), right.keySet())) {
							AggregationDiff leftOps = left.get(aggregation);
							AggregationDiff rightOps = right.get(aggregation);
							if (leftOps == null) leftOps = AggregationDiff.empty();
							if (rightOps == null) rightOps = AggregationDiff.empty();

							DiffPair<AggregationDiff> transformed = aggregationOTSystem.transform(DiffPair.of(leftOps, rightOps));

							if (transformed.left.size() > 1)
								throw new AssertionError();
							if (transformed.right.size() > 1)
								throw new AssertionError();

							if (!transformed.left.isEmpty())
								newOpsLeft.put(aggregation, transformed.left.get(0));
							if (!transformed.right.isEmpty())
								newOpsRight.put(aggregation, transformed.right.get(0));
						}
						return DiffPair.of(CubeDiff.of(newOpsLeft), CubeDiff.of(newOpsRight));
					}
				})
				.withEmptyPredicate(CubeDiff.class, new OTSystemImpl.EmptyPredicate<CubeDiff>() {
					@Override
					public boolean isEmpty(CubeDiff commit) {
						return commit.isEmpty();
					}
				})
				.withInvertFunction(CubeDiff.class, new OTSystemImpl.InvertFunction<CubeDiff>() {
					@Override
					public List<? extends CubeDiff> invert(CubeDiff commit) {
						return singletonList(commit.inverse());
					}
				})
				.withSquashFunction(CubeDiff.class, CubeDiff.class, new OTSystemImpl.SquashFunction<CubeDiff, CubeDiff, CubeDiff>() {
					@Override
					public CubeDiff trySquash(CubeDiff commit1, CubeDiff commit2) {
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
					}
				});

	}
}
