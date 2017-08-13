package io.datakernel.ot;

import java.util.List;

public interface OTSystem<D> {

	DiffPair<D> transform(DiffPair<? extends D> pair);

	List<D>[] transform(List<? extends D>[] inputs);

	List<D> squash(List<? extends D> ops);

	boolean isEmpty(D op);

	List<D> invert(List<? extends D> ops);
}
