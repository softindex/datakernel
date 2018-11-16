package io.datakernel.csp;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;

import java.util.Iterator;

import static io.datakernel.util.Recyclable.deepRecycle;

public final class ChannelConsumers {
	private ChannelConsumers() {}

	public static <T> Promise<Void> acceptAll(ChannelConsumer<T> output, Iterator<? extends T> it) {
		if (!it.hasNext()) return Promise.complete();
		SettablePromise<Void> result = new SettablePromise<>();
		acceptAllImpl(output, it, result);
		return result;
	}

	private static <T> void acceptAllImpl(ChannelConsumer<T> output, Iterator<? extends T> it, SettablePromise<Void> cb) {
		while (it.hasNext()) {
			Promise<Void> accept = output.accept(it.next());
			if (accept.isResult()) continue;
			accept.whenComplete(($, e) -> {
				if (e == null) {
					acceptAllImpl(output, it, cb);
				} else {
					deepRecycle(it);
					cb.setException(e);
				}
			});
			return;
		}
		cb.set(null);
	}

}
