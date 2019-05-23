package io.datakernel.di;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

public abstract class Key<T> {
	@NotNull
	private final Type type;
	@Nullable
	private final Name name;

	public Key(@Nullable Name name) {
		this.name = name;
		this.type = getSuperclassTypeParameter(getClass());
	}

	public Key() {
		this(null);
	}

	private Key(@NotNull Type type, @Nullable Name name) {
		this.type = type;
		this.name = name;
	}

	// so that we have one reusable non-abstract impl
	private static <T> Key<T> create(Type type, Name name) {
		return new Key<T>(type, name) {};
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @Nullable Name name) {
		return create(type, name);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type) {
		return create(type, null);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull Class<? extends Annotation> annotationType) {
		return create(type, Name.of(annotationType));
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull Annotation annotation) {
		return create(type, Name.of(annotation));
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type) {
		return create(type, null);
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type, @Nullable Name name) {
		return create(type, name);
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type, @NotNull Class<? extends Annotation> annotationType) {
		return create(type, Name.of(annotationType));
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type, @NotNull Annotation annotation) {
		return create(type, Name.of(annotation));
	}

	@NotNull
	private static Type getSuperclassTypeParameter(@NotNull Class<?> subclass) {
		Type superclass = subclass.getGenericSuperclass();
		if (superclass instanceof ParameterizedType) {
			return ((ParameterizedType) superclass).getActualTypeArguments()[0];
		}
		throw new IllegalArgumentException("Unsupported type: " + superclass);
	}

	@NotNull
	public Type getType() {
		return type;
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public Class<T> getRawType() {
		if (type instanceof Class) {
			return (Class<T>) type;
		} else if (type instanceof ParameterizedType) {
			return (Class<T>) ((ParameterizedType) type).getRawType();
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}

	public Type[] getTypeParams() {
		if (type instanceof ParameterizedType) {
			return ((ParameterizedType) type).getActualTypeArguments();
		}
		return new Type[0];
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
		return (name != null ? name.getDisplayString() + " " : "") + type.getTypeName().replaceAll("(?:\\w+\\.)*(\\w+)", "$1");
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
		System.out.println(new Key<List<String>>(Name.of("hello")) {}.getDisplayString());
	}
}
