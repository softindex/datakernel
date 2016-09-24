package io.datakernel.async;

public abstract class AssertingResultCallback<T> extends ResultCallback<T>  {
	@Override
	protected void onResult(T result) {
	}

	@Override
	protected final void onException(Exception e) {
		throw new AssertionError("Fatal error in callback " + this.getClass().getSimpleName(), e);
	}
}
