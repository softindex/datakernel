package io.datakernel.exception;

public final class UncheckedException extends RuntimeException {
	private UncheckedException(Throwable cause) {
		super(cause);
	}

	public static UncheckedException of(Exception checkedException) {
		return new UncheckedException(checkedException);
	}

	@Override
	public synchronized Throwable fillInStackTrace() {
		return this;
	}
}
