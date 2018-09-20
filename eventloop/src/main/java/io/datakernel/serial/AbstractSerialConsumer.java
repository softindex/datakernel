package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;

public abstract class AbstractSerialConsumer<T> implements SerialConsumer<T> {
	private static final Cancellable COMPLETE = e -> {
		throw new AssertionError();
	};

	@Nullable
	private Cancellable cancellable;

	protected AbstractSerialConsumer() {
		this.cancellable = null;
	}

	protected AbstractSerialConsumer(@Nullable Cancellable cancellable) {
		this.cancellable = cancellable;
	}

	protected void onClosed(Throwable e) {
	}

	@Override
	public final void closeWithError(Throwable e) {
		if (cancellable == COMPLETE) return;
		Cancellable cancellable = this.cancellable;
		this.cancellable = COMPLETE;
		onClosed(e);
		if (cancellable != null) {
			cancellable.closeWithError(e);
		}
	}

	@Override
	public final void cancel() {
		SerialConsumer.super.cancel();
	}
}
