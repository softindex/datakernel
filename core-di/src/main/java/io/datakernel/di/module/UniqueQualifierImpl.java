package io.datakernel.di.module;

import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

@SuppressWarnings("ClassExplicitlyAnnotation")
public final class UniqueQualifierImpl implements UniqueQualifier {
	@Nullable
	private final Object originalQualifier;

	public UniqueQualifierImpl() {
		this.originalQualifier = null;
	}

	public UniqueQualifierImpl(@Nullable Object originalQualifier) {
		this.originalQualifier = originalQualifier;
	}

	@Nullable
	public Object getOriginalQualifier() {
		return originalQualifier;
	}

	@Override
	public Class<? extends Annotation> annotationType() {
		return UniqueQualifier.class;
	}

	@Override
	public String toString() {
		return "@" + Integer.toHexString(hashCode()) + (originalQualifier != null ? " " + originalQualifier : "");
	}
}
