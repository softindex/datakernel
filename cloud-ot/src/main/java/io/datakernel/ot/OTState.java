package io.datakernel.ot;

import io.datakernel.async.Promise;

public interface OTState<D> {
	Promise<Void> init();

	Promise<Void> apply(D op);
}
