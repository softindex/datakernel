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

/**
 * The key defines an identity of a binding. In any DI, a key is usually a type of the object along
 * with some optional tag to distinguish between bindings which make objects of the same type.
 * <p>
 * In DataKernel DI, a key is also a type token - special abstract class that can store type information
 * with shortest syntax possible in Java.
 * <p>
 * For example, to create a key of type Map&lt;String, List&lt;Integer&gt;&gt;, you can just use
 * this syntax: <code>new Key&lt;Map&lt;String, List&lt;Integer&gt;&gt;&gt;(){}</code>.
 * <p>
 * If your types are not known at compile time, you can use {@link Types#parameterized} to make a
 * parameterized type and give it to a {@link #ofType Key.ofType} constructor.
 */
public abstract class Key<T> {
	@NotNull
	private final Type type;
	@Nullable
	private final Name name;

	public Key() {
		this.type = ensureEquality(getTypeParameter());
		this.name = null;
	}

	public Key(@Nullable Name name) {
		this.type = ensureEquality(getTypeParameter());
		this.name = name;
	}

	public Key(@NotNull String name) {
		this.type = ensureEquality(getTypeParameter());
		this.name = Name.of(name);
	}

	public Key(@NotNull Class<? extends Annotation> annotationType) {
		this.type = ensureEquality(getTypeParameter());
		this.name = Name.of(annotationType);
	}

	public Key(@NotNull Annotation annotation) {
		this.type = ensureEquality(getTypeParameter());
		this.name = Name.of(annotation);
	}

	Key(@NotNull Type type, @Nullable Name name) {
		this.type = ensureEquality(type);
		this.name = name;
	}

	/**
	 * A default subclass to be used by {@link #of Key.of*} and {@link #ofType Key.ofType*} constructors
	 */
	private static final class KeyImpl<T> extends Key<T> {
		private KeyImpl(Type type, Name name) {
			super(type, name);
		}
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

	/**
	 * Returns a new key with same type but the name replaced with a given one
	 */
	public Key<T> named(Name name) {
		return new KeyImpl<>(type, name);
	}

	/**
	 * @see #named(Name)
	 */
	public Key<T> named(String name) {
		return new KeyImpl<>(type, Name.of(name));
	}

	/**
	 * @see #named(Name)
	 */
	public Key<T> named(@NotNull Class<? extends Annotation> annotationType) {
		return new KeyImpl<>(type, Name.of(annotationType));
	}

	/**
	 * @see #named(Name)
	 */
	public Key<T> named(@NotNull Annotation annotation) {
		return new KeyImpl<>(type, Name.of(annotation));
	}

	@NotNull
	private Type getTypeParameter() {
		// this cannot possibly fail so not even a check here
		Type typeArgument = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
		Object outerInstance = ReflectionUtils.getOuterClassInstance(this);
		// the outer instance is null in static context
		return outerInstance != null ? Types.resolveTypeVariables(typeArgument, outerInstance.getClass(), outerInstance) : typeArgument;
	}

	@NotNull
	public Type getType() {
		return type;
	}

	/**
	 * A shortcut for <code>{@link Types#getRawType}(key.getType())</code>.
	 * Also casts the result to a properly parameterized class.
	 */
	@SuppressWarnings("unchecked")
	@NotNull
	public Class<T> getRawType() {
		return (Class<T>) Types.getRawType(type);
	}

	/**
	 * Returns a type parameter of the underlying type wrapped as a key with no name.
	 *
	 * @throws IllegalStateException when underlying type is not a parameterized one.
	 */
	public <U> Key<U> getTypeParameter(int index) {
		if (type instanceof ParameterizedType) {
			return new KeyImpl<>(((ParameterizedType) type).getActualTypeArguments()[index], null);
		}
		throw new IllegalStateException("Expected type from key " + getDisplayString() + " to be parameterized");
	}

	/**
	 * Null-checked shortcut for <code>key.getName()?.getAnnotationType()</code>.
	 */
	@Nullable
	public Class<? extends Annotation> getAnnotationType() {
		return name != null ? name.getAnnotationType() : null;
	}

	/**
	 * Null-checked shortcut for <code>key.getName()?.getAnnotation()</code>.
	 */
	@Nullable
	public Annotation getAnnotation() {
		return name != null ? name.getAnnotation() : null;
	}

	@Nullable
	public Name getName() {
		return name;
	}

	/**
	 * Returns an underlying type with display string formatting (package names stripped)
	 * and prepended name display string if this key has a name.
	 */
	public String getDisplayString() {
		return (name != null ? name.getDisplayString() + " " : "") + ReflectionUtils.getDisplayName(type);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Key)) {
			return false;
		}
		Key<?> that = (Key<?>) o;
		return type.equals(that.type) && Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return 31 * type.hashCode() + (name == null ? 0 : name.hashCode());
	}

	@Override
	public String toString() {
		return (name != null ? name.toString() + " " : "") + type.getTypeName();
	}
}
