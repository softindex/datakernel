package io.datakernel.async.process;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

public interface AsyncExecutor {
	@NotNull <T> Promise<T> execute(@NotNull AsyncSupplier<T> supplier) throws RejectedExecutionException;

	@NotNull
	default Promise<Void> run(@NotNull Runnable runnable) throws RejectedExecutionException {
		return execute(() -> {
			runnable.run();
			return Promise.complete();
		});
	}

	@NotNull
	default <T> Promise<T> call(@NotNull Callable<T> callable) throws RejectedExecutionException {
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
