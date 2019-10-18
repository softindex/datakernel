package io.global.pixel.ot;

import org.jetbrains.annotations.NotNull;

public final class UserId implements Comparable<UserId> {
	private final AuthService authService;
	private final String authId;

	public UserId(AuthService authService, String authId) {
		this.authService = authService;
		this.authId = authId;
	}

	public AuthService getAuthService() {
		return authService;
	}

	public String getAuthId() {
		return authId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		UserId userId = (UserId) o;
		return authService == userId.authService && authId.equals(userId.authId);
	}

	@Override
	public int hashCode() {
		return 31 * authService.hashCode() + authId.hashCode();
	}

	@Override
	public int compareTo(@NotNull UserId o) {
		int result = authService.compareTo(o.authService);
		if (result != 0) return result;
		return authId.compareTo(o.authId);
	}
}

