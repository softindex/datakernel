package io.global.forum.pojo;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import static io.global.forum.pojo.ForumPrivilege.*;

public enum UserRole implements Comparable<UserRole> {
	NONE,
	COMMON(CREATE_THREAD, EDIT_OWN_THREAD, SEE_OWN_DELETED_THREADS, CREATE_POST, EDIT_OWN_POST, SEE_OWN_DELETED_POSTS, RATE_POST),
	SUPER(COMMON, SEE_ANY_DELETED_THREADS, SEE_ANY_DELETED_POSTS, BAN_USER),
	OWNER(EnumSet.allOf(ForumPrivilege.class));

	private final EnumSet<ForumPrivilege> privileges;

	UserRole(ForumPrivilege... privileges) {
		this.privileges = EnumSet.noneOf(ForumPrivilege.class);
		this.privileges.addAll(Arrays.asList(privileges));
	}

	UserRole(UserRole cloneFrom, ForumPrivilege... privileges) {
		this.privileges = EnumSet.noneOf(ForumPrivilege.class);
		this.privileges.addAll(Arrays.asList(privileges));
		this.privileges.addAll(cloneFrom.privileges);
	}

	UserRole(EnumSet<ForumPrivilege> privileges) {
		this.privileges = privileges;
	}

	public Set<ForumPrivilege> getPrivileges() {
		return privileges;
	}

	public boolean has(ForumPrivilege privilege) {
		return privileges.contains(privilege);
	}
}
