package io.global.common;

import io.datakernel.common.parse.ParseException;

public final class RawServerId {
	private final String serverIdString;
	private final int priority;

	public RawServerId(String serverIdString, int priority) {
		this.serverIdString = serverIdString;
		this.priority = priority;
	}

	public RawServerId(String serverIdString) {
		this(serverIdString, 0);
	}

	public static RawServerId parse(String serverIdString, int priority) throws ParseException {
		return new RawServerId(serverIdString, priority); // TODO
	}

	public String getServerIdString() {
		return serverIdString;
	}

	public int getPriority() {
		return priority;
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
