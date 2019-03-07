package io.datakernel.async;

class Utils {
	private Utils() {}

	static <T> void forEachRemainingImpl(AsyncSupplier<T> promises, AsyncConsumer<T> consumer, SettableCallback<Void> cb) {
		promises.get()
				.whenComplete((v, e) -> {
					if (cb.isComplete()) return;
					if (e == null) {
						if (v != null) {
							consumer.accept(v)
									.whenComplete(($, e2) -> {
										if (cb.isComplete()) return;
										if (e2 == null) {
											forEachRemainingImpl(promises, consumer, cb);
										} else {
											cb.setException(e2);
										}
									});
						} else {
							cb.set(null);
						}
					} else {
						cb.setException(e);
					}
				});
	}

}
