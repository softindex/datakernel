package io.datakernel.exception;

public final class UncheckedException extends RuntimeException {
	public UncheckedException(Throwable cause) {
		super(cause);
	}

	@Override
	public synchronized Throwable fillInStackTrace() {
		return this;
	}

	@SuppressWarnings("unchecked")
	public <E extends Throwable> E propagate(Class<? extends E> exceptionType) {
		Throwable cause = getCause();
		if (exceptionType.isAssignableFrom(cause.getClass())) {
			return (E) cause;
		}
		throw this;
	}
}
