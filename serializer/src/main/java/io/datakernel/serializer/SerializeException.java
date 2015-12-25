package io.datakernel.serializer;

public class SerializeException extends Exception {

	public SerializeException() {
	}

	public SerializeException(String message) {
		super(message);
	}

	public SerializeException(String message, Throwable cause) {
		super(message, cause);
	}

	public SerializeException(Throwable cause) {
		super(cause);
	}

}
