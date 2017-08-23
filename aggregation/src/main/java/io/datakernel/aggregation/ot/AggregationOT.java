package io.datakernel.aggregation.ot;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.TransformResult.ConflictResolution;

import static com.google.common.collect.Sets.intersection;
import static io.datakernel.util.Preconditions.check;
import static java.util.Collections.singletonList;

public class AggregationOT {
	public static OTSystem<AggregationDiff> createAggregationOT() {
		return OTSystemImpl.<AggregationDiff>create()
				.withTransformFunction(AggregationDiff.class, AggregationDiff.class, (left, right) -> {
					check(intersection(left.getAddedChunks(), right.getAddedChunks()).isEmpty());

					if (intersection(left.getRemovedChunks(), right.getRemovedChunks()).isEmpty())
						return TransformResult.of(right, left);

					return left.getRemovedChunks().size() > right.getRemovedChunks().size() ?
							TransformResult.conflict(ConflictResolution.LEFT) :
							TransformResult.conflict(ConflictResolution.RIGHT);
				})
				.withInvertFunction(AggregationDiff.class, op -> singletonList(op.inverse()))
				.withEmptyPredicate(AggregationDiff.class, AggregationDiff::isEmpty)
				.withSquashFunction(AggregationDiff.class, AggregationDiff.class, AggregationDiff::squash);
	}
}
