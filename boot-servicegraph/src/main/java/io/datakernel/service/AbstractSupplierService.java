package io.datakernel.service;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.service.Utils.completedExceptionallyFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

public abstract class AbstractSupplierService<V> implements SupplierService<V> {
	@Nullable
	private V value;

	private final Executor executor;

	public AbstractSupplierService() {
		this(Runnable::run);
	}

	public AbstractSupplierService(Executor executor) {
		this.executor = executor;
	}

	@Override
	public final V get() {
		return checkNotNull(value);
	}

	@Override
	public final CompletableFuture<?> start() {
		CompletableFuture<Object> future = new CompletableFuture<>();
		executor.execute(() -> {
			try {
				this.value = checkNotNull(compute());
				future.complete(null);
			} catch (Exception e) {
				future.completeExceptionally(e);
			}
		});
		return future;
	}

	@NotNull
	protected abstract V compute() throws Exception;

	@Override
	public final CompletableFuture<?> stop() {
		try {
			onStop(value);
			return completedFuture(null);
		} catch (Exception e) {
			return completedExceptionallyFuture(e);
		} finally {
			value = null;
		}
	}

	@SuppressWarnings("WeakerAccess")
	protected void onStop(V value) throws Exception {
	}
}
