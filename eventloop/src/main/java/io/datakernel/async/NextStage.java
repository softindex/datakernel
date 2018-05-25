package io.datakernel.async;

import java.util.function.BiConsumer;

public abstract class NextStage<T, R> extends AbstractStage<R> implements BiConsumer<T, Throwable> {
	BiConsumer<? super T, Throwable> prev; // optimization

	protected abstract void onComplete(T result);

	protected void onCompleteExceptionally(Throwable throwable) {
		completeExceptionally(throwable);
	}

	@Override
	public final void accept(T t, Throwable throwable) {
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
