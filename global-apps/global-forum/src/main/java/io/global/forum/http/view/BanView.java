package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.BanState;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

import static io.global.forum.util.Utils.formatInstant;

public final class BanView {
	private final UserView banner;
	private final Instant untilInstant;
	private final String until;
	private final String reason;

	private BanView(UserView banner, Instant untilInstant, String until, String reason) {
		this.banner = banner;
		this.untilInstant = untilInstant;
		this.until = until;
		this.reason = reason;
	}

	public UserView getBanner() {
		return banner;
	}

	public String getUntil() {
		return until;
	}

	public Instant getUntilInstant() {
		return untilInstant;
	}

	public String getReason() {
		return reason;
	}

	public static Promise<BanView> from(CommDao commDao, @Nullable BanState state) {
		if (state == null) {
			return Promise.of(null);
		}
		return UserView.from(commDao, state.getBanner())
				.map(user -> {
					Instant until = state.getUntil();
					if (until.compareTo(Instant.now()) < 0) {
						return null;
					}
					return new BanView(user, until, formatInstant(until), state.getReason());
				});
	}
}
