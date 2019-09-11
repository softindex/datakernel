package io.datakernel.promise;

public interface Async<T> {
	Promise<T> get();
}
