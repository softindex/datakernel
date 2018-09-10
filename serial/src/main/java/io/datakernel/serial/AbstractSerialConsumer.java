package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;

public abstract class AbstractSerialConsumer<T> implements SerialConsumer<T> {
	private static final Cancellable CANCELLED = e -> {
		throw new AssertionError();
	};

	@Nullable
	Cancellable cancellable;

	protected AbstractSerialConsumer() {
		this.cancellable = null;
	}

	protected AbstractSerialConsumer(@Nullable Cancellable cancellable) {
		this.cancellable = cancellable;
	}

	protected void onClose(Throwable e) {
	}

	@Override
	public final void closeWithError(Throwable e) {
		if (cancellable == CANCELLED) return;
		Cancellable cancellable = this.cancellable;
		this.cancellable = CANCELLED;
		onClose(e);
		if (cancellable != null) {
			cancellable.closeWithError(e);
		}
	}

	@Override
	public final void cancel() {
		SerialConsumer.super.cancel();
	}
}
