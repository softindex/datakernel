package io.datakernel.exception;

public class TruncatedDataException extends ParseException {
	public TruncatedDataException() {
	}

	public TruncatedDataException(String message) {
		super(message);
	}

	public TruncatedDataException(String message, Throwable cause) {
		super(message, cause);
	}

	public TruncatedDataException(Throwable cause) {
		super(cause);
	}
}
