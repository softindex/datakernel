package io.global.ot.shared;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTState;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public final class SharedReposOTState implements OTState<SharedReposOperation> {
	public static final Consumer<SharedReposOperation> NO_ACTION = op -> {};

	private final Set<SharedRepo> sharedRepos = new HashSet<>();
	private Consumer<SharedReposOperation> listener = NO_ACTION;

	@Override
	public Promise<Void> init() {
		sharedRepos.clear();
		return Promise.complete();
	}

	@Override
	public Promise<Void> apply(SharedReposOperation op) {
		if (op.isEmpty()) {
			return Promise.complete();
		}

		if (op.isRemove()) {
			sharedRepos.remove(op.getSharedRepo());
		} else {
			sharedRepos.add(op.getSharedRepo());
		}
		listener.accept(op);

		return Promise.complete();
	}

	public Set<SharedRepo> getSharedRepos() {
		return sharedRepos;
	}

	public void setListener(Consumer<SharedReposOperation> listener) {
		this.listener = listener;
	}
}
