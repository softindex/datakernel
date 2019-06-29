package io.datakernel.service;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public interface SupplierService<V> extends Supplier<V>, Service {

	static <V> SupplierService<V> of(Callable<? extends V> callable) {
		return of(Runnable::run, callable);
	}

	static <V> SupplierService<V> of(Executor executor, Callable<? extends V> callable) {
		return new AbstractSupplierService<V>(executor) {
			@NotNull
			@Override
			protected V compute() throws Exception {
				return callable.call();
			}
		};
	}

}
