package io.global.forum.pojo;

public enum UserRole {
	COMMON(false),
	SUPER(true),
	ADMIN(true);

	private final boolean rights;

	UserRole(boolean rights) {
		this.rights = rights;
	}

	// this is not just a stub, but a SUPER STUB
	public boolean hasSufficientRights() {
		return rights;
	}
}
