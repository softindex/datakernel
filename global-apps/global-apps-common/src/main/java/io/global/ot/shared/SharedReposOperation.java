package io.global.ot.shared;

import static java.util.Collections.emptySet;

public class SharedReposOperation {
	public static final SharedReposOperation EMPTY = new SharedReposOperation(new SharedRepo("", emptySet()), true);

	private final SharedRepo sharedRepo;
	private final boolean remove;

	public SharedReposOperation(SharedRepo sharedRepo, boolean remove) {
		this.sharedRepo = sharedRepo;
		this.remove = remove;
	}

	public static SharedReposOperation create(SharedRepo sharedRepo) {
		return new SharedReposOperation(sharedRepo, false);
	}

	public static SharedReposOperation delete(SharedRepo sharedRepo) {
		return new SharedReposOperation(sharedRepo, true);
	}

	public SharedReposOperation invert() {
		return new SharedReposOperation(sharedRepo, !remove);
	}

	public boolean isEmpty() {
		return sharedRepo.getParticipants().isEmpty();
	}

	public boolean isRemove() {
		return remove;
	}

	public SharedRepo getSharedRepo() {
		return sharedRepo;
	}

	public boolean isInversionFor(SharedReposOperation other) {
		return sharedRepo.equals(other.sharedRepo) && remove != other.remove;
	}
}
