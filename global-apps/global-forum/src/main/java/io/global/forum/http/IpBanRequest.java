package io.global.forum.http;

import io.global.forum.pojo.IpRange;

import java.time.Instant;

public final class IpBanRequest {
	private final IpRange range;
	private final Instant until;
	private final String description;

	public IpBanRequest(IpRange range, Instant until, String description) {
		this.range = range;
		this.until = until;
		this.description = description;
	}

	public IpRange getRange() {
		return range;
	}

	public Instant getUntil() {
		return until;
	}

	public String getDescription() {
		return description;
	}
}
