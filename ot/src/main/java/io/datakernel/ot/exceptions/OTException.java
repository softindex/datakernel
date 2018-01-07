package io.datakernel.ot.exceptions;

public class OTException extends Exception {
	public OTException() {
	}

	public OTException(String message) {
		super(message);
	}

	public OTException(String message, Throwable cause) {
		super(message, cause);
	}

	public OTException(Throwable cause) {
		super(cause);
	}
}
