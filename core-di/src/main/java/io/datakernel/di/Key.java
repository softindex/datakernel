package io.datakernel.di;

import io.datakernel.util.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Objects;

public final class Key<T> {
	@NotNull
	private final TypeT<T> type;
	@Nullable
	private final Name name;

	protected Key(@NotNull TypeT<T> type, @Nullable Name name) {
		this.type = type;
		this.name = name;
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type) {
		return new Key<>(TypeT.of(type), null);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull TypeT<T> type) {
		return new Key<>(type, null);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull Name name) {
		return new Key<>(TypeT.of(type), name);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull Class<? extends Annotation> annotationType) {
		return new Key<>(TypeT.of(type), Name.of(annotationType));
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull Annotation annotation) {
		return new Key<>(TypeT.of(type), Name.of(annotation));
	}

	@NotNull
	public static <T> Key<T> of(@NotNull TypeT<T> type, @NotNull Name name) {
		return new Key<>(type, name);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull TypeT<T> type, @NotNull Class<? extends Annotation> annotationType) {
		return new Key<>(type, Name.of(annotationType));
	}

	@NotNull
	public static <T> Key<T> of(@NotNull TypeT<T> type, @NotNull Annotation annotation) {
		return new Key<>(type, Name.of(annotation));
	}

	@NotNull
	public TypeT<T> getTypeT() {
		return type;
	}

	@Nullable
	public Class<? extends Annotation> getAnnotationType() {
		return name != null ? name.getAnnotationType() : null;
	}

	@Nullable
	public Annotation getAnnotation() {
		return name != null ? name.getAnnotation() : null;
	}

	@Nullable
	public Name getName() {
		return name;
	}

	public String getDisplayString() {
		return (name != null ? name.getDisplayString() + " " : "") + type.getDisplayString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Key<?> key = (Key<?>) o;

		return Objects.equals(type, key.type) && Objects.equals(name, key.name);

	}

	@Override
	public int hashCode() {
		return 31 * type.hashCode() + (name != null ? name.hashCode() : 0);
	}

	@Override
	public String toString() {
		return (name != null ? name.toString() : "") + type;
	}

	public static void main(String[] args) {
		System.out.println(Key.of(new TypeT<List<String>>(){}, Name.of("hello")).getDisplayString());
	}
}
