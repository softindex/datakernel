package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import io.datakernel.functional.Try;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class NextStage<T, R> extends AbstractStage<R> implements BiConsumer<T, Throwable> {
	BiConsumer<? super T, Throwable> prev; // optimization

	protected abstract void onComplete(@Nullable T result);

	protected void onCompleteExceptionally(Throwable throwable) {
		completeExceptionally(throwable);
	}

	@Override
	public final void accept(@Nullable T t, @Nullable Throwable throwable) {
		if (prev != null) {
			prev.accept(t, throwable);
		}
		if (throwable == null) {
			onComplete(t);
		} else {
			onCompleteExceptionally(throwable);
		}
	}

	@Override
	public boolean isSet() {
		return false;
	}

	@Override
	public boolean isResult() {
		return false;
	}

	@Override
	public boolean isException() {
		return false;
	}

	@Override
	public R getResult() {
		throw new IllegalStateException();
	}

	@Override
	public Throwable getException() {
		throw new IllegalStateException();
	}

	@Override
	public Try<R> getTry() {
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
}
