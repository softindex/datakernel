package io.datakernel.vlog.handler;


import io.datakernel.async.callback.Callback;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ProgressListenerImpl implements ProgressListener {
	private final Map<String, Long> processedTasks = new ConcurrentHashMap<>();
	private final AtomicLong fullCapacity = new AtomicLong();
	private final int listenersCount;
	private final int progressListenersCount;
	private final Callback<Void> callback;
	private final AtomicInteger completeCount = new AtomicInteger();

	public ProgressListenerImpl(int listenersCount, int progressListenersCount, Callback<Void> callback) {
		this.listenersCount = listenersCount;
		this.progressListenersCount = progressListenersCount;
		this.callback = callback;
	}

	@Override
	public void onProgress(String taskId, long progress) {
		processedTasks.put(taskId, progress);
	}

	@Override
	public void onError(Throwable e) {
		callback.accept(null, e);
	}

	@Override
	public void onComplete() {
		int count = completeCount.incrementAndGet();
		if (count == listenersCount) {
			callback.accept(null, null);
		}
	}

	public Double getProgress() {
		double fullSize = fullCapacity.get();
		if (fullSize == 0) {
			return 0.0;
		}
		return processedTasks.values()
				.stream()
				.reduce(0L, Long::sum) / fullSize;
	}

	@Override
	public void trySetProgressLimit(long limit) {
		fullCapacity.compareAndSet(0, limit * progressListenersCount);
	}
}

