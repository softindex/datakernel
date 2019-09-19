package io.global.ot.shared;

import io.global.ot.name.ChangeName;

import java.util.Set;

import static io.global.ot.name.ChangeName.changeName;

public final class RenameRepo implements SharedReposOperation {
	private final String id;
	private final ChangeName changeNameOp;

	private RenameRepo(String id, ChangeName changeNameOp) {
		this.id = id;
		this.changeNameOp = changeNameOp;
	}

	public static RenameRepo of(String id, ChangeName changeNameOp) {
		return new RenameRepo(id, changeNameOp);
	}

	public static RenameRepo of(String id, String prev, String next, long timestamp) {
		return new RenameRepo(id, new ChangeName(prev, next, timestamp));
	}

	@Override
	public void apply(Set<SharedRepo> repos) {
		for (SharedRepo repo : repos) {
			if (repo.getId().equals(id)) {
				repo.setName(changeNameOp.getNext());
			}
		}
	}

	public String getId() {
		return id;
	}

	@Override
	public boolean isEmpty() {
		return changeNameOp.isEmpty();
	}

	@Override
	public SharedReposOperation invert() {
		return new RenameRepo(id, changeName(changeNameOp.getNext(), changeNameOp.getPrev(), changeNameOp.getTimestamp()));
	}

	public ChangeName getChangeNameOp() {
		return changeNameOp;
	}
}
