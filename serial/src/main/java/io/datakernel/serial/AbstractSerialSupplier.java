package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;

public abstract class AbstractSerialSupplier<T> implements SerialSupplier<T> {
	private static final Cancellable CANCELLED = e -> {
		throw new AssertionError();
	};

	@Nullable
	Cancellable cancellable;

	protected AbstractSerialSupplier() {
		this.cancellable = null;
	}

	protected AbstractSerialSupplier(@Nullable Cancellable cancellable) {
		this.cancellable = cancellable;
	}

	protected void onClosed(Throwable e) {
	}

	@Override
	public final void closeWithError(Throwable e) {
		if (cancellable == CANCELLED) return;
		Cancellable cancellable = this.cancellable;
		this.cancellable = CANCELLED;
		onClosed(e);
		if (cancellable != null) {
			cancellable.closeWithError(e);
		}
	}

	@Override
	public final void cancel() {
		SerialSupplier.super.cancel();
	}
}
