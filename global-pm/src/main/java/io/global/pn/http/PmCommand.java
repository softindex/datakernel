package io.global.pn.http;

import io.datakernel.http.HttpPathPart;

public enum PmCommand implements HttpPathPart {
	SEND,
	MULTISEND,
	POLL,
	MULTIPOLL,
	DROP,
	MULTIDROP;

	@Override
	public String toString() {
		return name().toLowerCase();
	}
}
