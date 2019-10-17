package io.global.comm.pojo;

import io.global.ot.session.UserId;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Objects;

public final class BanState implements Comparable<BanState> {
	private final UserId banner;
	private final Instant until;
	private final String reason;

	public BanState(UserId banner, Instant until, String reason) {
		this.banner = banner;
		this.until = until;
		this.reason = reason;
	}

	public UserId getBanner() {
		return banner;
	}

	public Instant getUntil() {
		return until;
	}

	public String getReason() {
		return reason;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BanState state = (BanState) o;

		if (!Objects.equals(banner, state.banner)) return false;
		if (!Objects.equals(until, state.until)) return false;
		return Objects.equals(reason, state.reason);
	}

	@Override
	public int hashCode() {
		int result = banner != null ? banner.hashCode() : 0;
		result = 31 * result + (until != null ? until.hashCode() : 0);
		result = 31 * result + (reason != null ? reason.hashCode() : 0);
		return result;
	}

	@Override
	public int compareTo(@NotNull BanState o) {
		int result = banner.compareTo(o.banner);
		if (result != 0) return result;
		result = until.compareTo(o.until);
		if (result != 0) return result;
		return reason.compareTo(o.reason);
	}
}
