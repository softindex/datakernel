package io.datakernel.serializer;

public class SerializeSizeException extends SerializeException {

	public SerializeSizeException() {
	}

	public SerializeSizeException(String message) {
		super(message);
	}

	public SerializeSizeException(String message, Throwable cause) {
		super(message, cause);
	}

	public SerializeSizeException(Throwable cause) {
		super(cause);
	}

}
