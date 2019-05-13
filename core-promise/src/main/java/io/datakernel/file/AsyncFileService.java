package io.datakernel.file;

import io.datakernel.async.Promise;
import io.datakernel.util.ApplicationSettings;


import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;

public interface AsyncFileService {
	AsyncFileService DEFAULT_FILE_SERVICE = getDefaultInstance();

	static AsyncFileService getDefaultInstance() {
		int threadPoolSize = ApplicationSettings.getInt(ExecutorAsyncFileService.class, "thread-pool-size", 1);

		return new ExecutorAsyncFileService(Executors.newFixedThreadPool(threadPoolSize));
	}

	Promise<Integer> read(FileChannel channel, long position, byte[] array, int offset, int size);

	Promise<Integer> write(FileChannel channel, long position, byte[] array, int offset, int size);
}
