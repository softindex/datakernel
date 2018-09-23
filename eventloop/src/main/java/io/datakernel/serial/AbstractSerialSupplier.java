package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;

public abstract class AbstractSerialSupplier<T> implements SerialSupplier<T> {
	private static final Cancellable CLOSED = e -> {
		throw new AssertionError();
	};

	@Nullable
	private Cancellable cancellable;

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
		if (isClosed()) return;
		Cancellable cancellable = this.cancellable;
		this.cancellable = CLOSED;
		onClosed(e);
		if (cancellable != null) {
			cancellable.closeWithError(e);
		}
	}

	@Override
	public final void cancel() {
		SerialSupplier.super.cancel();
	}

	@Override
	public final void close() {
		SerialSupplier.super.close();
	}

	public final boolean isClosed() {
		return cancellable == CLOSED;
	}

}
