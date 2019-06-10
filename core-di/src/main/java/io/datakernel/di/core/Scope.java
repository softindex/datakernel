package io.datakernel.di.core;

import io.datakernel.di.annotation.ScopeAnnotation;
import io.datakernel.di.util.AbstractAnnotation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

import static io.datakernel.di.util.Utils.checkArgument;

public final class Scope extends AbstractAnnotation {
	protected Scope(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		super(annotationType, annotation);
	}

	public static Scope of(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType), "Scope by annotation type only accepts marker annotations with no arguments");
		checkArgument(annotationType.isAnnotationPresent(ScopeAnnotation.class), "Only annotations annotated with @ScopeAnnotation meta-annotation are allowed");
		return new Scope(annotationType, null);
	}

	public static Scope of(Annotation annotation) {
		Class<? extends Annotation> annotationType = annotation.annotationType();
		checkArgument(annotationType.isAnnotationPresent(ScopeAnnotation.class), "Only annotations annotated with @ScopeAnnotation meta-annotation are allowed");
		return isMarker(annotationType) ?
				new Scope(annotationType, null) :
				new Scope(annotationType, annotation);
	}
}
