package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;

class Utils {
	static <T> void retryImpl(AsyncSupplier<? extends T> supplier, RetryPolicy retryPolicy,
			int retryCount, long _retryTimestamp, SettableStage<T> cb) {
		supplier.get().whenComplete((value, throwable) -> {
			if (throwable == null) {
				cb.set(value);
			} else {
				Eventloop eventloop = Eventloop.getCurrentEventloop();
				long now = eventloop.currentTimeMillis();
				long retryTimestamp = _retryTimestamp != 0 ? _retryTimestamp : now;
				long nextRetryTimestamp = retryPolicy.nextRetryTimestamp(now, throwable, retryCount, retryTimestamp);
				if (nextRetryTimestamp == 0) {
					cb.setException(throwable);
				} else {
					eventloop.schedule(nextRetryTimestamp,
							() -> retryImpl(supplier, retryPolicy, retryCount + 1, retryTimestamp, cb));
				}
			}
		});
	}

}
