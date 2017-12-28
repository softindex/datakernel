package io.datakernel.ot.exceptions;

public class OTException extends Exception {
	public OTException() {
	}

	public OTException(final String message) {
		super(message);
	}

	public OTException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public OTException(final Throwable cause) {
		super(cause);
	}
}
