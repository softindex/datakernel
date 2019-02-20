package io.datakernel.async;

public interface AsyncFunction0<R> {
	Promise<R> call();
}
