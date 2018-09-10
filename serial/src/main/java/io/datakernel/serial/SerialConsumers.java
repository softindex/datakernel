package io.datakernel.serial;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import java.util.Iterator;

import static io.datakernel.util.Recyclable.deepRecycle;

public final class SerialConsumers {
	private SerialConsumers() {}

	public static <T> Stage<Void> acceptAll(SerialConsumer<T> output, Iterator<? extends T> it) {
		SettableStage<Void> result = new SettableStage<>();
		acceptAllImpl(output, it, result);
		return result;
	}

	private static <T> void acceptAllImpl(SerialConsumer<T> output, Iterator<? extends T> it, SettableStage<Void> cb) {
		while (it.hasNext()) {
			Stage<Void> accept = output.accept(it.next());
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
