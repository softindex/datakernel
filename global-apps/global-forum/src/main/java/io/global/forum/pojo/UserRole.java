package io.global.forum.pojo;

public enum UserRole {
	COMMON,
	SUPER,
	ADMIN,
	OWNER;

	// this is not just a stub, but a SUPER STUB
	public boolean hasSufficientRights() {
		return ordinal() > 0;
	}
}
