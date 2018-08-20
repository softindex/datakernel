package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;

public abstract class AbstractSerialConsumer<T> implements SerialConsumer<T> {
	@Nullable
	private Cancellable cancellable;

	protected AbstractSerialConsumer() {
		this.cancellable = null;
	}

	protected AbstractSerialConsumer(@Nullable Cancellable cancellable) {
		this.cancellable = cancellable;
	}

	@Override
	public void closeWithError(Throwable e) {
		if (cancellable != null) {
			Cancellable cancellable = this.cancellable;
			this.cancellable = null;
			cancellable.closeWithError(e);
		}
	}

	@Override
	public void cancel() {
		if (cancellable != null) {
			Cancellable cancellable = this.cancellable;
			this.cancellable = null;
			cancellable.cancel();
		}
	}
}
