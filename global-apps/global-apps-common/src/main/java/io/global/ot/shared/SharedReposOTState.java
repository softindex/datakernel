package io.global.ot.shared;

import io.datakernel.ot.OTState;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public final class SharedReposOTState implements OTState<SharedReposOperation> {
	public static final Consumer<CreateOrDropRepo> NO_ACTION = op -> {};

	private final Set<SharedRepo> sharedRepos = new HashSet<>();
	private Consumer<CreateOrDropRepo> listener = NO_ACTION;

	@Override
	public void init() {
		sharedRepos.clear();
	}

	@Override
	public void apply(SharedReposOperation op) {
		if (op.isEmpty()) return;
		op.apply(sharedRepos);
		if (op instanceof CreateOrDropRepo) {
			listener.accept((CreateOrDropRepo) op);
		}
	}

	public Set<SharedRepo> getSharedRepos() {
		return sharedRepos;
	}

	public void setListener(Consumer<CreateOrDropRepo> listener) {
		this.listener = listener;
	}
}
