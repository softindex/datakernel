package io.datakernel.aggregation.ot;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.TransformResult.ConflictResolution;

import static io.datakernel.common.collection.CollectionUtils.hasIntersection;
import static java.util.Collections.singletonList;

public class AggregationOT {
	public static OTSystem<AggregationDiff> createAggregationOT() {
		return OTSystemImpl.<AggregationDiff>create()
				.withTransformFunction(AggregationDiff.class, AggregationDiff.class, (left, right) -> {
					if (!hasIntersection(left.getAddedChunks(), right.getAddedChunks()) && !hasIntersection(left.getRemovedChunks(), right.getRemovedChunks())) {
						return TransformResult.of(right, left);
					}

					return left.getRemovedChunks().size() > right.getRemovedChunks().size() ?
							TransformResult.conflict(ConflictResolution.LEFT) :
							TransformResult.conflict(ConflictResolution.RIGHT);
				})
				.withInvertFunction(AggregationDiff.class, op -> singletonList(op.inverse()))
				.withEmptyPredicate(AggregationDiff.class, AggregationDiff::isEmpty)
				.withSquashFunction(AggregationDiff.class, AggregationDiff.class, AggregationDiff::squash);
	}

}
