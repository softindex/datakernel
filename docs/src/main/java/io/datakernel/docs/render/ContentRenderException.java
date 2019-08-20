package io.datakernel.docs.render;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class ContentRenderException extends Exception {
	private List<Exception> causeExceptionList;

	public ContentRenderException(String message) {
		super(message);
	}

	public ContentRenderException() {
		super();
	}

	public List<Exception> getCauseExceptionList() {
		return causeExceptionList;
	}

	public boolean hasReasonToThrow() {
		return causeExceptionList != null || getMessage() != null;
	}

	public void wrapException(ExceptionalRunnable task) {
		try {
			task.call();
		} catch (Exception e) {
			addCauseException(e);
		}
	}

	public void addCauseException(@NotNull Exception exception) {
		if (causeExceptionList == null) {
			causeExceptionList = new ArrayList<>();
		}
		causeExceptionList.add(exception);
	}

	public interface ExceptionalRunnable {
		void call() throws Exception;
	}
}
