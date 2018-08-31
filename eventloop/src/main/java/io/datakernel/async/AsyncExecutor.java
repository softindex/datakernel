package io.datakernel.async;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

public interface AsyncExecutor {
	<T> Stage<T> execute(AsyncSupplier<T> supplier) throws RejectedExecutionException;

	default Stage<Void> run(Runnable runnable) throws RejectedExecutionException {
		return execute(() -> {
			runnable.run();
			return Stage.complete();
		});
	}

	default <T> Stage<T> call(Callable<T> callable) throws RejectedExecutionException {
		return execute(() -> {
			T result;
			try {
				result = callable.call();
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				return Stage.ofException(e);
			}
			return Stage.of(result);
		});
	}
}
