package io.datakernel.async;

public interface AsyncExecutor {
	void submit(AsyncTask asyncTask, CompletionCallback callback);

	boolean isSaturated();
}
