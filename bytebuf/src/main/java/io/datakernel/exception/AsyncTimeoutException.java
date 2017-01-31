package io.datakernel.exception;

public class AsyncTimeoutException extends SimpleException {
	public AsyncTimeoutException() {
	}

	public AsyncTimeoutException(String s) {
		super(s);
	}
}
