package io.datakernel.di.error;

import java.lang.reflect.AnnotatedElement;

public final class InvalidAnnotationException extends IllegalStateException {
	private final AnnotatedElement annotatedElement;

	public InvalidAnnotationException(AnnotatedElement annotatedElement, String message) {
		super(message + " on " + annotatedElement);
		this.annotatedElement = annotatedElement;
	}

	public AnnotatedElement getAnnotatedElement() {
		return annotatedElement;
	}
}
