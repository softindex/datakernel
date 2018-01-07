package io.datakernel.aggregation.ot;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.TransformResult.ConflictResolution;
import io.datakernel.ot.exceptions.OTTransformException;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singletonList;

public class AggregationOT {
	public static OTSystem<AggregationDiff> createAggregationOT() {
		return OTSystemImpl.<AggregationDiff>create()
				.withTransformFunction(AggregationDiff.class, AggregationDiff.class, (left, right) -> {
					Set<AggregationChunk> intersection = intersection(left.getAddedChunks(), right.getAddedChunks());

					if (!intersection.isEmpty()) {
						throw new OTTransformException(String.format("Added chunks intersection is not empty." +
								"Left added chunks: %s, right added chunks: %s, intersection: %s",
								left.getAddedChunks(), right.getAddedChunks(), intersection));
					}

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

	private static <T> Set<T> intersection(Set<T> a, Set<T> b) {
		Set<T> set = new HashSet<>();
		for (T x : a) {
			if (b.contains(x)) set.add(x);
		}
		return set;
	}

}
