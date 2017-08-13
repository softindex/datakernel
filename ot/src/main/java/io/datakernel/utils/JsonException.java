package io.datakernel.utils;

public class JsonException extends Exception {
	public JsonException(String message) {
		super(message);
	}

	public JsonException(Throwable cause) {
		super(cause);
	}
}
