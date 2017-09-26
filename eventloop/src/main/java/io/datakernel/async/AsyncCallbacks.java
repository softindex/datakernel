package io.datakernel.async;

@SuppressWarnings("WeakerAccess")
public class AsyncCallbacks {
	private AsyncCallbacks() {
	}

	public static Exception throwableToException(Throwable throwable) {
		return throwable instanceof Exception ? (Exception) throwable : new RuntimeException(throwable);
	}

}
