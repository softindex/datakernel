package io.datakernel.async;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public abstract class NextStage<F, T> extends AbstractStage<T> {
	protected abstract void onComplete(F result);

	protected abstract void onCompleteExceptionally(Throwable throwable);

	public static <T> NextStage<T, T> exceptionally(Function<? super Throwable, ? extends T> fn) {
		return new NextStage<T, T>() {
			@Override
			protected void onComplete(T result) {
				complete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				complete(fn.apply(throwable));
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

	private static final Object NO_RESULT = new Object();

	@SuppressWarnings("unchecked")
	private static class CombineStage<T, V, U> extends NextStage<T, V> {
		final BiFunction<? super T, ? super U, ? extends V> fn;
		T thisResult = (T) NO_RESULT;
		U otherResult = (U) NO_RESULT;

		CombineStage(BiFunction<? super T, ? super U, ? extends V> fn) {
			this.fn = fn;
		}

		@Override
		protected void onComplete(T thisResult) {
			if (otherResult != NO_RESULT) {
				onBothResults(thisResult, otherResult);
			} else {
				this.thisResult = thisResult;
			}
		}

		protected void onOtherComplete(U otherResult) {
			if (thisResult != NO_RESULT) {
				onBothResults(thisResult, otherResult);
			} else {
				this.otherResult = otherResult;
			}
		}

		void onBothResults(T thisResult, U otherResult) {
			if (!isComplete()) {
				complete(fn.apply(thisResult, otherResult));
			}
		}

		void onAnyException(Throwable throwable) {
			tryCompleteExceptionally(throwable);
		}

		@Override
		protected void onCompleteExceptionally(Throwable throwable) {
			onAnyException(throwable);
		}
	}

	public static <T, U, V> NextStage<T, V> combine(Stage<? extends U> other,
	                                                BiFunction<? super T, ? super U, ? extends V> fn) {
		CombineStage<T, V, U> resultStage = new CombineStage<>(fn);
		other.then(new NextStage<U, Object>() {
			@Override
			protected void onComplete(U result) {
				resultStage.onOtherComplete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				resultStage.onAnyException(throwable);
			}
		});
		return resultStage;
	}

	public static <T, U, V> NextStage<T, V> combineAsync(Stage<? extends U> other,
	                                                     BiFunction<? super T, ? super U, ? extends V> fn) {
		CombineStage<T, V, U> resultStage = new CombineStage<T, V, U>(fn) {
			@Override
			void onBothResults(T thisResult, U otherResult) {
				if (!isComplete()) {
					getCurrentEventloop().post(() -> complete(fn.apply(thisResult, otherResult)));
				}
			}

			@Override
			void onAnyException(Throwable throwable) {
				getCurrentEventloop().post(() -> tryCompleteExceptionally(throwable));
			}
		};
		other.then(new NextStage<U, Object>() {
			@Override
			protected void onComplete(U result) {
				resultStage.onOtherComplete(result);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				resultStage.onAnyException(throwable);
			}
		});
		return resultStage;
	}

}
