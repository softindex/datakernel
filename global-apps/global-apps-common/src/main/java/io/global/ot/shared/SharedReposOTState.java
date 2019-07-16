package io.global.ot.shared;

import io.datakernel.ot.OTState;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public final class SharedReposOTState implements OTState<SharedReposOperation> {
	public static final Consumer<SharedReposOperation> NO_ACTION = op -> {};

	private final Set<SharedRepo> sharedRepos = new HashSet<>();
	private Consumer<SharedReposOperation> listener = NO_ACTION;

	@Override
	public void init() {
		sharedRepos.clear();
	}

	@Override
	public void apply(SharedReposOperation op) {
		if (op.isEmpty()) return;

		if (op.isRemove()) {
			sharedRepos.remove(op.getSharedRepo());
		} else {
			sharedRepos.add(op.getSharedRepo());
		}
		listener.accept(op);
	}

	public Set<SharedRepo> getSharedRepos() {
		return sharedRepos;
	}

	public void setListener(Consumer<SharedReposOperation> listener) {
		this.listener = listener;
	}
}
