package io.global.forum.pojo;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.ofEnum;

public final class UserId {
	public static final StructuredCodec<UserId> CODEC = StructuredCodecs.tuple(UserId::new,
			UserId::getAuthService, ofEnum(AuthService.class),
			UserId::getAuthString, STRING_CODEC);

	private final AuthService authService;
	private final String authString;

	public UserId(AuthService authService, String authString) {
		this.authService = authService;
		this.authString = authString;
	}

	public AuthService getAuthService() {
		return authService;
	}

	public String getAuthString() {
		return authString;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		UserId userId = (UserId) o;

		if (authService != userId.authService) return false;
		if (!authString.equals(userId.authString)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = authService.hashCode();
		result = 31 * result + authString.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "UserId{" +
				"authService=" + authService +
				", authString='" + authString + '\'' +
				'}';
	}
}

