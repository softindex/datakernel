package io.datakernel.ot.exceptions;

public class OTTransformException extends OTException {
	public OTTransformException() {
	}

	public OTTransformException(String message) {
		super(message);
	}

	public OTTransformException(String message, Throwable cause) {
		super(message, cause);
	}

	public OTTransformException(Throwable cause) {
		super(cause);
	}
}
