package io.datakernel.async;

import io.datakernel.functional.Try;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class NextStage<T, R> extends AbstractStage<R> implements BiConsumer<T, Throwable> {
	@Override
	public boolean isMaterialized() {
		return false;
	}

	@Override
	public boolean hasResult() {
		return false;
	}

	@Override
	public boolean hasException() {
		return false;
	}

	@Override
	public R getResult() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Throwable getException() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Try<R> asTry() {
		return null;
	}

	@Override
	public boolean setTo(BiConsumer<? super R, Throwable> consumer) {
		return false;
	}

	@Override
	public boolean setResultTo(Consumer<? super R> consumer) {
		return false;
	}

	@Override
	public boolean setExceptionTo(Consumer<Throwable> consumer) {
		return false;
	}

	@Override
	public Stage<R> async() {
		return this;
	}

}
