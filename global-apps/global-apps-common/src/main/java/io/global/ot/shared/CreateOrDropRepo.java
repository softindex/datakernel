package io.global.ot.shared;

import io.global.common.PubKey;

import java.util.Set;

import static java.util.Collections.emptySet;

public class CreateOrDropRepo implements SharedReposOperation {
	public static final CreateOrDropRepo EMPTY = new CreateOrDropRepo(new SharedRepo("", "", emptySet()), true);

	private final SharedRepo sharedRepo;
	private final boolean remove;

	public CreateOrDropRepo(SharedRepo sharedRepo, boolean remove) {
		this.sharedRepo = sharedRepo;
		this.remove = remove;
	}

	public static CreateOrDropRepo create(SharedRepo sharedRepo) {
		return new CreateOrDropRepo(sharedRepo, false);
	}

	public static CreateOrDropRepo drop(SharedRepo sharedRepo) {
		return new CreateOrDropRepo(sharedRepo, true);
	}

	@Override
	public void apply(Set<SharedRepo> repos) {
		if (remove) {
			repos.remove(sharedRepo);
		} else {
			repos.add(sharedRepo);
		}
	}

	@Override
	public CreateOrDropRepo invert() {
		return new CreateOrDropRepo(sharedRepo, !remove);
	}

	@Override
	public String getId() {
		return sharedRepo.getId();
	}

	public String getName() {
		return sharedRepo.getName();
	}

	public Set<PubKey> getParticipants() {
		return sharedRepo.getParticipants();
	}

	@Override
	public boolean isEmpty() {
		return sharedRepo.getParticipants().isEmpty();
	}

	public boolean isRemove() {
		return remove;
	}

	public SharedRepo getSharedRepo() {
		return sharedRepo;
	}

	public boolean isInversionFor(CreateOrDropRepo other) {
		return sharedRepo.equals(other.sharedRepo) && remove != other.remove;
	}
}
