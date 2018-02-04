package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

public interface ReduceListener<T, A, R> {
	interface ReduceCanceller {
		void cancel();

		void cancel(Throwable throwable);
	}

	void onStart(ReduceCanceller canceller, A accumulator);

	default void onResult(T result, int index) {
	}

	default void onFinish(R result) {
	}

	default void onException(Throwable throwable) {
	}

	default ReduceListener<T, A, R> and(ReduceListener<T, A, R> other) {
		return new ReduceListener<T, A, R>() {
			@Override
			public void onStart(ReduceCanceller canceller, A accumulator) {
				ReduceListener.this.onStart(canceller, accumulator);
				other.onStart(canceller, accumulator);
			}

			@Override
			public void onResult(T result, int index) {
				ReduceListener.this.onResult(result, index);
				other.onResult(result, index);
			}

			@Override
			public void onFinish(R result) {
				ReduceListener.this.onFinish(result);
				other.onFinish(result);
			}

			@Override
			public void onException(Throwable throwable) {
				ReduceListener.this.onException(throwable);
				other.onException(throwable);
			}
		};
	}

	static <T, A, R> ReduceListener<T, A, R> timeout(long timeout) {
		Stages.ReduceTimeouter<T, A, R> timeouter = new Stages.ReduceTimeouter<>();
		timeouter.scheduledRunnable = Eventloop.getCurrentEventloop().delay(timeout, timeouter);
		return timeouter;
	}

	static <T, A, R> ReduceListener<T, A, R> any(int results) {
		return new ReduceListener<T, A, R>() {
			ReduceCanceller canceller;
			int counter = results;

			@Override
			public void onStart(ReduceCanceller canceller, A accumulator) {
				this.canceller = canceller;
			}

			@Override
			public void onResult(T result, int index) {
				if (--counter == 0) {
					canceller.cancel();
				}
			}
		};
	}
}
