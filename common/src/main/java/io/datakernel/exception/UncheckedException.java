package io.datakernel.exception;

public final class UncheckedException extends RuntimeException {
	public UncheckedException(Throwable cause) {
		super(cause);
	}

	@Override
	public synchronized Throwable fillInStackTrace() {
		return this;
	}

	@FunctionalInterface
	public interface ThrowableSupplier<T> {

		T get() throws Throwable;
	}

	@FunctionalInterface
	public interface ThrowableRunnable {

		void run() throws Throwable;
	}

	public static <T> T unchecked(ThrowableSupplier<T> supplier) {
		try {
			return supplier.get();
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Throwable e) {
			throw new UncheckedException(e);
		}
	}

	public static void unchecked(ThrowableRunnable runnable) {
		try {
			runnable.run();
		} catch (RuntimeException | Error e) {
			throw e;
		} catch (Throwable e) {
			throw new UncheckedException(e);
		}
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
