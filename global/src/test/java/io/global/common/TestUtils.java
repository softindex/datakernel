package io.global.common;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestUtils {
	public static <T> T await(Promise<T> promise) {
		try {
			return compute(promise);
		} catch (ExecutionException e) {
			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - e.getCause() is used
			throw new AssertionError(e.getCause());
		}
	}

	public static <T> Throwable awaitException(Promise<T> promise) {
		try {
			compute(promise);
		} catch (ExecutionException e) {
			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - e.getCause() is used
			return e.getCause();
		}
		throw new AssertionError("Promise did not fail");
	}

	private static <T> T compute(Promise<T> promise) throws ExecutionException {
		Future<T> future = promise.toCompletableFuture();
		Eventloop.getCurrentEventloop().run();
		try {
			return future.get();
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		}
	}
}
