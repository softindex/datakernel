package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;

public abstract class AbstractSerialConsumer<T> implements SerialConsumer<T> {
	private static final Cancellable CLOSED = e -> {
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
		SerialConsumer.super.cancel();
	}

	@Override
	public final void close() {
		SerialConsumer.super.close();
	}

	public final boolean isClosed() {
		return cancellable == CLOSED;
	}

}
