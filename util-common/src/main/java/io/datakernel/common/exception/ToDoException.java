package io.datakernel.common.exception;

public class ToDoException extends RuntimeException {

	public ToDoException() {
	}

	public ToDoException(String message, Throwable cause) {
		super(message, cause);
	}

	public ToDoException(String s) {
		super(s);
	}

	public ToDoException(Throwable cause) {
		super(cause);
	}
}
