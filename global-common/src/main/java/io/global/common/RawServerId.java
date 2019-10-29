package io.global.common;

import io.datakernel.common.parse.ParseException;

public final class RawServerId {
	private final String serverIdString;

	public RawServerId(String serverIdString) {
		this.serverIdString = serverIdString;
	}

	public static RawServerId parse(String serverIdString) throws ParseException {
		return new RawServerId(serverIdString); // TODO
	}

	public String getServerIdString() {
		return serverIdString;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RawServerId that = (RawServerId) o;

		return serverIdString.equals(that.serverIdString);
	}

	@Override
	public int hashCode() {
		return serverIdString.hashCode();
	}

	@Override
	public String toString() {
		return "RawServerId{" + serverIdString + '}';
	}
}
