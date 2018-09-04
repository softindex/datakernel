package io.datakernel.async;

public final class CompleteNullStage<T> extends CompleteStage<T> {
	public static final CompleteNullStage<?> INSTANCE = new CompleteNullStage<>();

	@SuppressWarnings("unchecked")
	static <T> CompleteNullStage<T> instance() {
		return (CompleteNullStage<T>) INSTANCE;
	}

	private CompleteNullStage() {
	}

	@Override
	public final T getResult() {
		return null;
	}

}
