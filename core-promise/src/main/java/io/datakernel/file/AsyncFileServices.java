package io.datakernel.file;

import java.util.concurrent.Executor;

public abstract class AsyncFileServices {
	public static AsyncFileService getDefaultInstance(Executor executor) {
		return new ExecutorAsyncFileService(executor);
	}
}
