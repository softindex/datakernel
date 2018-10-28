package io.datakernel.async;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

public interface AsyncExecutor {
	<T> Promise<T> execute(AsyncSupplier<T> supplier) throws RejectedExecutionException;

	default Promise<Void> run(Runnable runnable) throws RejectedExecutionException {
		return execute(() -> {
			runnable.run();
			return Promise.complete();
		});
	}

	default <T> Promise<T> call(Callable<T> callable) throws RejectedExecutionException {
		return execute(() -> {
			T result;
			try {
				result = callable.call();
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				return Promise.ofException(e);
			}
			return Promise.of(result);
		});
	}
}
