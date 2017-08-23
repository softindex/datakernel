package io.datakernel.ot;

import java.util.List;

import static java.util.Collections.singletonList;

public interface OTSystem<D> {

	TransformResult<D> transform(List<? extends D> leftDiffs, List<? extends D> rightDiffs);

	default TransformResult<D> transform(D leftDiff, D rightDiff) {
		return transform(singletonList(leftDiff), singletonList(rightDiff));
	}

	List<D> squash(List<? extends D> ops);

	boolean isEmpty(D op);

	List<D> invert(List<? extends D> ops);
}
