package io.global.pm.http;

import io.datakernel.http.HttpPathPart;

public enum PmCommand implements HttpPathPart {
	UPLOAD,
	DOWNLOAD,
	SEND,
	POLL,
	LIST,
	DROP,
	STREAM,
	MULTISEND,
	MULTIPOLL,
	MULTIDROP,
	BATCHPOLL;

	@Override
	public String toString() {
		return name().toLowerCase();
	}
}
