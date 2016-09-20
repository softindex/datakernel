package io.datakernel;

import io.datakernel.exception.SimpleException;

public class RemoteFsException extends SimpleException {
	public RemoteFsException() {
	}

	public RemoteFsException(String message, Throwable cause) {
		super(message, cause);
	}

	public RemoteFsException(String s) {
		super(s);
	}

	public RemoteFsException(Throwable cause) {
		super(cause);
	}
}
