package io.datakernel.exception;

import org.jetbrains.annotations.NotNull;

public final class UncheckedException extends RuntimeException {
	public UncheckedException(@NotNull Throwable cause) {
		super(cause);
	}

	@Override
	public Throwable fillInStackTrace() {
		return this;
	}

	@SuppressWarnings("unchecked")
	public <E extends Throwable> E propagate(@NotNull Class<? extends E> exceptionType) {
		Throwable cause = getCause();
		if (exceptionType.isAssignableFrom(cause.getClass())) {
			return (E) cause;
		}
		throw this;
	}
}
