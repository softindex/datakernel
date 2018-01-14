package io.datakernel.stream.processor;

public interface StreamStats {
	interface Receiver<T> {
		void onData(T item);
	}

	void onStarted();

	void onProduce();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable throwable);
}
