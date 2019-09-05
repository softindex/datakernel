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

	public static Name uniqueName() {
		return Name.of(new UniqueNameImpl());
	}

	public static Name uniqueName(@Nullable Name name) {
		return name != null && name.getAnnotation() instanceof UniqueNameImpl ?
				name :
				Name.of(new UniqueNameImpl(name));
	}

	@Nullable
	public Name getOriginalName() {
		return originalName;
	}

	@Override
	public int dummy() {
		return 0;
	}

	@Override
	public Class<? extends Annotation> annotationType() {
		return UniqueName.class;
	}

	@Override
	public String toString() {
		return "@" + hashCode() + (originalName != null ? " " + originalName : "");
	}
}
