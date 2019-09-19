package io.global.comm.pojo;

public enum UserRole implements Comparable<UserRole> {
	GUEST,
	COMMON,
	OWNER;

	public boolean isKnown() {
		return this != GUEST;
	}

	public boolean isPrivileged() {
		return this == OWNER;
	}
}
