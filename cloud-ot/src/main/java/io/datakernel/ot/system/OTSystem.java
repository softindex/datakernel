package io.datakernel.ot.system;

import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;

import java.util.List;

import static java.util.Collections.singletonList;

public interface OTSystem<D> {

	TransformResult<D> transform(List<? extends D> leftDiffs, List<? extends D> rightDiffs) throws OTTransformException;

	default TransformResult<D> transform(D leftDiff, D rightDiff) throws OTTransformException {
		return transform(singletonList(leftDiff), singletonList(rightDiff));
	}

	List<D> squash(List<? extends D> ops);

	boolean isEmpty(D op);

	<O extends D> List<D> invert(List<O> ops);
}
