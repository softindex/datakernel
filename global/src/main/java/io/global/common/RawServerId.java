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
}
