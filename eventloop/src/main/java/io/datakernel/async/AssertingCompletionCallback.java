package io.datakernel.async;

public abstract class AssertingCompletionCallback extends CompletionCallback  {
	@Override
	protected final void onException(Exception e) {
		throw new AssertionError("Fatal error in callback " + this.getClass().getSimpleName(), e);
	}
}
