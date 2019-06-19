package io.global.ot.shared;

import io.datakernel.ot.OTState;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class SharedReposOTState implements OTState<SharedReposOperation> {
	public static final Consumer<CreateOrDropRepos> NO_ACTION = op -> {};

	private final Map<String, SharedRepo> sharedRepos = new HashMap<>();
	private Consumer<CreateOrDropRepos> listener = NO_ACTION;

	@Override
	public void init() {
		sharedRepos.clear();
	}

	@Override
	public void apply(SharedReposOperation op) {
		op.apply(sharedRepos);
		if (op instanceof CreateOrDropRepos) {
			listener.accept((CreateOrDropRepos) op);
		}
	}

	public Map<String, SharedRepo> getSharedRepos() {
		return sharedRepos;
	}

	public void setListener(Consumer<CreateOrDropRepos> listener) {
		this.listener = listener;
	}
}
