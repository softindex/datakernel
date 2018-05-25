package io.datakernel.util;

@FunctionalInterface
public interface ThrowingRunnable {
	void run() throws Throwable;
}
