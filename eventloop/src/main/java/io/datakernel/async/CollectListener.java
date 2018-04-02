package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

public interface CollectListener<T, A, R> {
	interface CollectCanceller {
		void finish();

		void finishExceptionally(Throwable throwable);
	}

	void onStart(CollectCanceller canceller, A accumulator);

	default void onResult(T result, int index) {
	}

	default void onException(Throwable throwable, int index) {
	}

	default void onCollectResult(R result) {
	}

	default void onCollectException(Throwable throwable) {
	}

	static <T, A, R> CollectListener<T, A, R> timeout(long timeout) {
		Stages.ReduceTimeouter<T, A, R> timeouter = new Stages.ReduceTimeouter<>();
		timeouter.scheduledRunnable = Eventloop.getCurrentEventloop().delay(timeout, timeouter);
		return timeouter;
	}

	static <T, A, R> CollectListener<T, A, R> any(int results) {
		return new CollectListener<T, A, R>() {
			CollectCanceller canceller;
			int counter = results;

			@Override
			public void onStart(CollectCanceller canceller, A accumulator) {
				this.canceller = canceller;
			}

			@Override
			public void onResult(T result, int index) {
				if (--counter == 0) {
					canceller.finish();
				}
			}
		};
	}
}
