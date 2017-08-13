package io.datakernel.async;

public class AssertingCompletionCallback extends CompletionCallback  {
	@Override
	protected void onComplete() {
	}

	@Override
	protected final void onException(Exception e) {
		throw new AssertionError("Fatal error in callback " + this.getClass().getSimpleName(), e);
	}
}
