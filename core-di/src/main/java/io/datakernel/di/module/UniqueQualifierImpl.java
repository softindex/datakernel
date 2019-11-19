package io.datakernel.di.module;

import io.datakernel.di.core.Name;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

@SuppressWarnings("ClassExplicitlyAnnotation")
public final class UniqueQualifierImpl implements UniqueQualifier {
	@Nullable
	private final Name originalName;

	public UniqueQualifierImpl() {
		this.originalName = null;
	}

	public UniqueQualifierImpl(@Nullable Name originalName) {
		this.originalName = originalName;
	}

	@Nullable
	public Name getOriginalName() {
		return originalName;
	}

	@Override
	public Class<? extends Annotation> annotationType() {
		return UniqueQualifier.class;
	}

	@Override
	public String toString() {
		return "@" + Integer.toHexString(hashCode()) + (originalName != null ? " " + originalName : "");
	}
}
