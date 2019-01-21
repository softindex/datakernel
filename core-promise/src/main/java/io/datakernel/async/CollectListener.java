package io.datakernel.async;

import io.datakernel.async.Promises.ReduceTimeouter;
import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface CollectListener<T, A, R> {
	interface CollectCanceller {
		void finish();

		void finishExceptionally(@NotNull Throwable e);
	}

	void onStart(@NotNull CollectCanceller canceller, @Nullable A accumulator);

	default void onResult(@Nullable T promiseResult, int index) {
	}

	default void onException(@NotNull Throwable e, int index) {
	}

	default void onCollectResult(@Nullable R result) {
	}

	default void onCollectException(@NotNull Throwable e) {
	}

	static <T, A, R> CollectListener<T, A, R> timeout(long timeout) {
		ReduceTimeouter<T, A, R> timeouter = new ReduceTimeouter<>();
		timeouter.scheduledRunnable = Eventloop.getCurrentEventloop().delay(timeout, timeouter);
		return timeouter;
	}

	static <T, A, R> CollectListener<T, A, R> any(int results) {
		return new CollectListener<T, A, R>() {
			CollectCanceller canceller;
			int counter = results;

			@Override
			public void onStart(@NotNull CollectCanceller canceller, @Nullable A accumulator) {
				this.canceller = canceller;
			}

			@Override
			public void onResult(@Nullable T promiseResult, int index) {
				if (--counter == 0) {
					canceller.finish();
				}
			}
		};
	}
}
