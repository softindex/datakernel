package io.datakernel.async;

import io.datakernel.annotation.Nullable;

public abstract class AbstractCancellable implements Cancellable{
	@Nullable
	protected Cancellable cancellable;

	@Nullable
	protected Throwable exception;

	protected void onClosed(Throwable e){
	}

	@Override
	public final void closeWithError(Throwable e) {
		if (isClosed()) return;
		this.exception = e;
		onClosed(e);
		if (cancellable != null) {
			cancellable.closeWithError(e);
		}
	}

	public final boolean isClosed(){
		return exception != null;
	}
}
