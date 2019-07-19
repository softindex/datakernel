package io.datakernel.di.util;

import io.datakernel.di.core.Name;
import io.datakernel.di.core.Scope;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

import static io.datakernel.di.util.ReflectionUtils.isMarkerAnnotation;
import static io.datakernel.di.util.Utils.checkArgument;
import static java.util.Collections.emptyMap;

/**
 * This is a helper class for making tag-like objects that are identified by stateless or stateful annotations.
 * <p>
 * You don't need to extend it yourself.
 *
 * @see Name
 * @see Scope
 */
public abstract class AnnotationTag {
	private final Annotation annotation;

	protected AnnotationTag(Annotation annotation) {
		this.annotation = annotation;
	}

	protected AnnotationTag(Class<? extends Annotation> annotationType) {
		checkArgument(isMarkerAnnotation(annotationType), "Cannot express a stateful annotation only by its type");
		annotation = ReflectionUtils.createAnnotationInstance(annotationType, emptyMap());
	}

	@NotNull
	public Annotation getAnnotation() {
		return annotation;
	}

	public String getDisplayString() {
		return ReflectionUtils.getShortName(annotation.toString());
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AnnotationTag other = (AnnotationTag) o;
		return annotation.equals(other.annotation);
	}

	@Override
	public int hashCode() {
		return annotation.hashCode();
	}

	@Override
	public String toString() {
		return annotation.toString();
	}
}
