package io.global.ot.chat.common;

import io.datakernel.http.HttpPathPart;

public enum GatewayCommand implements HttpPathPart {
	CHECKOUT("checkout"),
	PULL("pull"),
	PUSH("push");

	private final String pathPart;

	GatewayCommand(String pathPart) {
		this.pathPart = pathPart;
	}

	@Override
	public String toString() {
		return pathPart;
	}
}
