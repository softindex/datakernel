package io.global.globalfs.api;

import io.datakernel.exception.StacklessException;

public class GlobalFsException extends StacklessException {

	public GlobalFsException() {
		super();
	}

	public GlobalFsException(String message, Throwable cause) {
		super(message, cause);
	}

	public GlobalFsException(String s) {
		super(s);
	}

	public GlobalFsException(Throwable cause) {
		super(cause);
	}
}
