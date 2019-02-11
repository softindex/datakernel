package io.datakernel.ot;

import io.datakernel.async.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public interface GraphVisitor<K, D> {
	default void onStart(@NotNull Collection<OTCommit<K, D>> heads) {
	}

	enum Status {
		CONTINUE,
		BREAK,
		SKIP_COMMIT
	}

	@NotNull
	Promise<Status> onCommit(@NotNull OTCommit<K, D> commit);
}
