package io.global.common;

import java.net.InetSocketAddress;

public final class RawServerId {
	private final InetSocketAddress inetSocketAddress;

	public RawServerId(InetSocketAddress inetSocketAddress) {
		this.inetSocketAddress = inetSocketAddress;
	}

	public InetSocketAddress getInetSocketAddress() {
		return inetSocketAddress;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RawServerId that = (RawServerId) o;

		return inetSocketAddress.equals(that.inetSocketAddress);
	}

	@Override
	public int hashCode() {
		return inetSocketAddress.hashCode();
	}

	@Override
	public String toString() {
		return "RawServerId{" + inetSocketAddress + '}';
	}
}
