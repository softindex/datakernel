package io.datakernel.vlog.handler;

public interface ProgressListener {
	void onProgress(String taskId, long progress);

	void onError(Throwable e);

	void onComplete();

	Double getProgress();

	void trySetProgressLimit(long limit);
}
