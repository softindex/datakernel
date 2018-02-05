package io.datakernel.async;

import java.util.function.Consumer;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public abstract class NextStage<T, R> extends AbstractStage<R> {
	protected abstract void onComplete(T result);

	protected abstract void onCompleteExceptionally(Throwable throwable);

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

	public static <T> NextStage<T, T> onError(Consumer<? super Throwable> consumer) {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				complete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				consumer.accept(throwable);
				completeExceptionally(throwable);
			}
		};
	}

	public static <T> NextStage<T, T> onError(Runnable action) {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				complete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				action.run();
				completeExceptionally(throwable);
			}
		};
	}

	public static <T> NextStage<T, T> onResult(Consumer<? super T> action) {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				action.accept(result);
				complete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		};
	}

	public static <T> NextStage<T, T> onResult(Runnable action) {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				action.run();
				complete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		};
	}


}
