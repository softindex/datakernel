package io.datakernel.ot.exceptions;

public class OTTransformException extends OTException {
	public OTTransformException() {
	}

	public OTTransformException(final String message) {
		super(message);
	}

	public OTTransformException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public OTTransformException(final Throwable cause) {
		super(cause);
	}
}
