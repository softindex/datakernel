package io.datakernel.exception;

public class UncheckedWrapperException extends RuntimeException {
	public UncheckedWrapperException(Throwable cause) {
		super(cause);
	}

	@Override
	public synchronized Throwable fillInStackTrace() {
		return this;
	}
}
