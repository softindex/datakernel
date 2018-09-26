package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AbstractCancellable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Stage;

public abstract class AbstractSerialSupplier<T> extends AbstractCancellable implements SerialSupplier<T> {
	protected AbstractSerialSupplier() {
		this.cancellable = null;
	}

	protected AbstractSerialSupplier(@Nullable Cancellable cancellable) {
		this.cancellable = cancellable;
	}

	protected abstract Stage<T> doGet();

	@Override
	public final Stage<T> get() {
		if (isClosed()) return Stage.ofException(exception);
		return doGet();
	}

	@Override
	public final void cancel() {
		SerialSupplier.super.cancel();
	}

	@Override
	public final void close() {
		SerialSupplier.super.close();
	}
}
