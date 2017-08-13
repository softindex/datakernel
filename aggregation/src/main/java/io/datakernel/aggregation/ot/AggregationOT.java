package io.datakernel.aggregation.ot;

import io.datakernel.ot.DiffPair;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import java.util.List;

import static com.google.common.collect.Sets.intersection;
import static io.datakernel.util.Preconditions.check;
import static java.util.Collections.singletonList;

public class AggregationOT {
	public static OTSystem<AggregationDiff> createAggregationOT() {
		return OTSystemImpl.<AggregationDiff>create()
				.withTransformFunction(AggregationDiff.class, AggregationDiff.class, new OTSystemImpl.TransformFunction<AggregationDiff, AggregationDiff, AggregationDiff>() {
					@Override
					public DiffPair<? extends AggregationDiff> transform(AggregationDiff left, AggregationDiff right) {
						check(intersection(left.getAddedChunks(), right.getAddedChunks()).isEmpty());

						if (intersection(left.getRemovedChunks(), right.getRemovedChunks()).isEmpty())
							return DiffPair.of(right, left);

						if (left.getRemovedChunks().size() > right.getRemovedChunks().size())
							return DiffPair.right(AggregationDiff.simplify(right.inverse(), left));
						else
							return DiffPair.left(AggregationDiff.simplify(left.inverse(), right));
					}
				})
				.withInvertFunction(AggregationDiff.class, new OTSystemImpl.InvertFunction<AggregationDiff>() {
					@Override
					public List<? extends AggregationDiff> invert(AggregationDiff op) {
						return singletonList(op.inverse());
					}
				})
				.withEmptyPredicate(AggregationDiff.class, new OTSystemImpl.EmptyPredicate<AggregationDiff>() {
					@Override
					public boolean isEmpty(AggregationDiff op) {
						return op.isEmpty();
					}
				})
				.withSquashFunction(AggregationDiff.class, AggregationDiff.class, new OTSystemImpl.SquashFunction<AggregationDiff, AggregationDiff, AggregationDiff>() {
					@Override
					public AggregationDiff trySquash(AggregationDiff commit1, AggregationDiff commit2) {
						return AggregationDiff.simplify(commit1, commit2);
					}
				});
	}
}
