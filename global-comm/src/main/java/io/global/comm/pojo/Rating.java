package io.global.comm.pojo;

import org.jetbrains.annotations.Nullable;

public enum Rating {
	LIKE, DISLIKE;

	@Nullable
	public static Rating fromString(String s) {
		try {
			return valueOf(s.toUpperCase());
		} catch (IllegalArgumentException ignored) {
			return null;
		}
	}
}
