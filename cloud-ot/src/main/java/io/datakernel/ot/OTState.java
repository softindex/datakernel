package io.datakernel.ot;

public interface OTState<D> {
	void init();

	void apply(D op);
}
