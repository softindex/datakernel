package io.global.forum.pojo;

import org.jetbrains.annotations.NotNull;

public final class UserId implements Comparable<UserId> {
	private final AuthService authService;
	private final String id;

	public UserId(AuthService authService, String id) {
		this.authService = authService;
		this.id = id;
	}

	public AuthService getAuthService() {
		return authService;
	}

	public String getId() {
		return id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		UserId userId = (UserId) o;

		return authService == userId.authService && id.equals(userId.id);
	}

	@Override
	public int hashCode() {
		return 31 * authService.hashCode() + id.hashCode();
	}

	@Override
	public int compareTo(@NotNull UserId o) {
		int result = authService.compareTo(o.authService);
		if (result != 0) return result;
		return id.compareTo(o.id);
	}
}

