package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AbstractCancellable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Stage;

import static io.datakernel.util.Recyclable.deepRecycle;

public abstract class AbstractSerialConsumer<T> extends AbstractCancellable implements SerialConsumer<T> {
	protected AbstractSerialConsumer() {
		this.cancellable = null;
	}

	protected AbstractSerialConsumer(@Nullable Cancellable cancellable) {
		this.cancellable = cancellable;
	}

	protected abstract Stage<Void> doAccept(@Nullable T value);

	@Override
	public Stage<Void> accept(@Nullable T value) {
		if (isClosed()) {
			deepRecycle(value);
			return Stage.ofException(exception);
		}
		return doAccept(value);
	}

	@Override
	public final void cancel() {
		SerialConsumer.super.cancel();
	}

	@Override
	public final void close() {
		SerialConsumer.super.close();
	}
}
