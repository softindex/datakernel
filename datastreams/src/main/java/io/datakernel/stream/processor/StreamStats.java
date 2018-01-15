package io.datakernel.stream.processor;

import io.datakernel.stream.StreamDataReceiver;

public interface StreamStats {
	<T> StreamDataReceiver<T> createDataReceiver(StreamDataReceiver<T> actualDataReceiver);

	void onStarted();

	void onProduce();

	void onSuspend();

	void onEndOfStream();

	void onError(Throwable throwable);
}
