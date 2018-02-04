package io.datakernel.net;

import io.datakernel.exception.StacklessException;

public class CloseWithoutNotifyException extends StacklessException {

	public CloseWithoutNotifyException() {
	}

	public CloseWithoutNotifyException(Throwable cause) {
		super(cause);
	}

	public CloseWithoutNotifyException(String message, Throwable cause) {
		super(message, cause);
	}

	public CloseWithoutNotifyException(String s) {
		super(s);
	}
}
