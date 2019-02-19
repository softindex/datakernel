package io.datakernel.ot;

import io.datakernel.async.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public interface GraphReducer<K, D, R> {
	default void onStart(@NotNull Collection<OTCommit<K, D>> queue) {
	}

	Object RESUME = new Object();

	@SuppressWarnings("unchecked")
	static <T> T resume() {return (T) RESUME;}

	Object SKIP = new Object();

	@SuppressWarnings("unchecked")
	static <T> T skip() {return (T) SKIP;}

	@NotNull
	Promise<R> onCommit(@NotNull OTCommit<K, D> commit);
}
