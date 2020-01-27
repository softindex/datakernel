package io.global.comm.http;

import io.global.comm.pojo.IpRange;

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
