package io.datakernel.async;

public interface MaterializedStage<T> extends Stage<T> {
	@Override
	default MaterializedStage<T> materialize() {
		return this;
	}

	@Override
	default boolean isMaterialized() {
		return true;
	}

	@Override
	default boolean hasResult() {
		return isResult();
	}

	@Override
	default boolean hasException() {
		return isException();
	}
}
