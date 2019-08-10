package io.datakernel.di.module;

import io.datakernel.di.core.Name;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

@SuppressWarnings("ClassExplicitlyAnnotation")
public final class UniqueNameImpl implements UniqueName {
	@Nullable
	private final Name originalName;

	public UniqueNameImpl() {
		this.originalName = null;
	}

	public UniqueNameImpl(@Nullable Name originalName) {
		this.originalName = originalName;
	}

	@Nullable
	public Name getOriginalName() {
		return originalName;
	}

	@Override
	public Class<? extends Annotation> annotationType() {
		return UniqueName.class;
	}

	@Override
	public String toString() {
		return "@" + Integer.toHexString(hashCode()) + (originalName != null ? " " + originalName : "");
	}
}
