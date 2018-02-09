package io.datakernel.async;

import java.util.function.BiConsumer;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public abstract class NextStage<T, R> extends AbstractStage<R> implements BiConsumer<T, Throwable> {
	protected abstract void onComplete(T result);

	protected void onCompleteExceptionally(Throwable throwable) {
		completeExceptionally(throwable);
	}

	@Override
	public final void accept(T t, Throwable throwable) {
		if (throwable == null) {
			onComplete(t);
		} else {
			onCompleteExceptionally(throwable);
		}
	}

	public static <T> NextStage<T, T> async() {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				getCurrentEventloop().post(() -> complete(result));
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				getCurrentEventloop().post(() -> completeExceptionally(throwable));
			}
		};
	}

	public static <T> NextStage<T, T> delay(long delay) {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				getCurrentEventloop().delay(delay, () -> complete(result));
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		};
	}

	public static <T> NextStage<T, T> schedule(long timestamp) {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				getCurrentEventloop().schedule(timestamp, () -> complete(result));
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		};
	}

	public static <T> NextStage<T, Void> toVoid() {
		return new NextStage<T, Void>() {
			@Override
			protected void onComplete(T result) {
				complete(null);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		};
	}

}
