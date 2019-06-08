package io.datakernel.di.core;

import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

import static io.datakernel.di.util.Types.ensureEquality;

public abstract class Key<T> {
	@NotNull
	private final Type type;
	@Nullable
	private final Name name;

	public Key() {
		this.type = ensureEquality(getSuperclassTypeParameter(getClass()));
		this.name = null;
	}

	public Key(@Nullable Name name) {
		this.type = ensureEquality(getSuperclassTypeParameter(getClass()));
		this.name = name;
	}

	public Key(@NotNull String name) {
		this.type = ensureEquality(getSuperclassTypeParameter(getClass()));
		this.name = Name.of(name);
	}

	public Key(@NotNull Class<? extends Annotation> annotationType) {
		this.type = ensureEquality(getSuperclassTypeParameter(getClass()));
		this.name = Name.of(annotationType);
	}

	public Key(@NotNull Annotation annotation) {
		this.type = ensureEquality(getSuperclassTypeParameter(getClass()));
		this.name = Name.of(annotation);
	}

	Key(@NotNull Type type, @Nullable Name name) {
		this.type = ensureEquality(type);
		this.name = name;
	}

	private static final class KeyImpl<T> extends Key<T> {
		KeyImpl(Type type, Name name) {super(type, name);}
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type) {
		return new KeyImpl<>(type, null);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @Nullable Name name) {
		return new KeyImpl<>(type, name);
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull String name) {
		return new KeyImpl<>(type, Name.of(name));
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull Class<? extends Annotation> annotationType) {
		return new KeyImpl<>(type, Name.of(annotationType));
	}

	@NotNull
	public static <T> Key<T> of(@NotNull Class<T> type, @NotNull Annotation annotation) {
		return new KeyImpl<>(type, Name.of(annotation));
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type) {
		return new KeyImpl<>(type, null);
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type, @Nullable Name name) {
		return new KeyImpl<>(type, name);
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type, @NotNull String name) {
		return new KeyImpl<>(type, Name.of(name));
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type, @NotNull Class<? extends Annotation> annotationType) {
		return new KeyImpl<>(type, Name.of(annotationType));
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull Type type, @NotNull Annotation annotation) {
		return new KeyImpl<>(type, Name.of(annotation));
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
		return (Class<T>) Types.getRawType(type);
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
		return (name != null ? name.getDisplayString() + " " : "") + ReflectionUtils.getShortName(type.getTypeName());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Key)) return false;
		Key<?> that = (Key<?>) o;
		return type.equals(that.type) && Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return 31 * type.hashCode() + (name != null ? name.hashCode() : 0);
	}

	@Override
	public String toString() {
		return (name != null ? name.toString() + " " : "") + type.getTypeName();
	}

}
