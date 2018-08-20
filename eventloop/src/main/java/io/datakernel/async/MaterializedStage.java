package io.datakernel.async;

public interface MaterializedStage<T> extends Stage<T> {
	@Override
	default MaterializedStage<T> materialize() {
		return this;
	}
}
