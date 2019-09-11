package io.datakernel.rpc.client.sender;

import io.datakernel.async.callback.Callback;

import java.util.concurrent.CompletableFuture;

public final class Callbacks {
	private Callbacks() {}

	public static <T> Callback<T> forFuture(CompletableFuture<T> future) {
		return (result, e) -> {
			if (e == null) {
				future.complete(result);
			} else {
				future.completeExceptionally(e);
			}
		};
	}

	public static <T> Callback<T> ignore() {
		return (result, e) -> {};
	}

	public static <T> Callback<T> assertNoCalls() {
		return (result, e) -> {throw new AssertionError();};
	}
}
