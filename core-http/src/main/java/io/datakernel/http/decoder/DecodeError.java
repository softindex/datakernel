package io.datakernel.http.decoder;

public final class DecodeError {
	final String message;
	final Object[] args;

	private DecodeError(String message, Object[] args) {
		this.message = message;
		this.args = args;
	}

	public static DecodeError of(String message, Object... args) {
		return new DecodeError(message, args);
	}

	public String getMessage() {
		return message;
	}

	public Object[] getArgs() {
		return args;
	}

	@Override
	public String toString() {
		return String.format(message, args);
	}
}
