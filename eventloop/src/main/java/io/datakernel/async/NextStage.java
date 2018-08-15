package io.datakernel.async;

import io.datakernel.annotation.Nullable;

import java.util.function.BiConsumer;

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

}
