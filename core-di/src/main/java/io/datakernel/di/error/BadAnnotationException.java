package io.datakernel.di.error;

import java.lang.annotation.Annotation;

public final class BadAnnotationException extends IllegalStateException {
	private final Annotation[] annotations;

	public BadAnnotationException(Annotation[] annotations, String message) {
		super(message);
		this.annotations = annotations;
	}

	public Annotation[] getAnnotations() {
		return annotations;
	}
}
