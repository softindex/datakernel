package io.datakernel.async.callback;

public interface AsyncComputation<T> {
	void run(Callback<? super T> callback);
}
