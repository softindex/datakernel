package io.global.ot.shared;

import io.global.ot.value.ChangeValue;

import java.util.Set;

public final class RenameRepo implements SharedReposOperation {
	private final String id;
	private final ChangeValue<String> changeNameOp;

	private RenameRepo(String id, ChangeValue<String> changeNameOp) {
		this.id = id;
		this.changeNameOp = changeNameOp;
	}

	public static RenameRepo of(String id, ChangeValue<String> changeNameOp) {
		return new RenameRepo(id, changeNameOp);
	}

	public static RenameRepo of(String id, String prev, String next, long timestamp) {
		return new RenameRepo(id, ChangeValue.of(prev, next, timestamp));
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
		return new RenameRepo(id, ChangeValue.of(changeNameOp.getNext(), changeNameOp.getPrev(), changeNameOp.getTimestamp()));
	}

	public ChangeValue<String> getChangeNameOp() {
		return changeNameOp;
	}
}
