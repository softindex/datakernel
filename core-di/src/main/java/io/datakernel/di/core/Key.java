package io.datakernel.di.core;

import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.util.Objects;

import static io.datakernel.di.util.ReflectionUtils.nameOf;

/**
 * The key defines an identity of a binding. In any DI, a key is usually a type of the object along
 * with some optional tag to distinguish between bindings which make objects of the same type.
 * <p>
 * In DataKernel DI, a key is also a type token - special abstract class that can store type information
 * with shortest syntax possible in Java.
 * <p>
 * For example, to create a key of type Map&lt;String, List&lt;Integer&gt;&gt;, you can just use
 * this syntax:<br>
 * <code>{@literal new Key<Map<String, List<Integer>>>(){}}</code>.
 * <p>
 * Starting from Java 9 you can even use name annotations when defining your keys in such way:<br>
 * <code>{@literal new Key<@Named("counter") Integer>(){}}</code>.
 * In Java 8 the reflection required for this is bugged.
 * <p>
 * If your types are not known at compile time, you can use {@link Types#parameterized} to make a
 * parameterized type and give it to a {@link #ofType Key.ofType} constructor.
 */
public abstract class Key<T> {
	@NotNull
	private final AnnotatedType annotatedType;

	@Nullable
	private Name name;

	Key(@NotNull AnnotatedType annotatedType) {
		this.annotatedType = annotatedType;
		name = nameOf(annotatedType);
	}

	Key(@NotNull AnnotatedType annotatedType, @Nullable Name name) {
		this.annotatedType = annotatedType;
		this.name = name;
	}

	public Key() {
		AnnotatedType annotatedType = retrieveType();
		this.annotatedType = annotatedType;
		name = nameOf(annotatedType);
	}

	public Key(@Nullable Name name) {
		annotatedType = name != null ? Types.annotate(retrieveType(), name.getAnnotation()) : retrieveType();
		this.name = name;
	}

	public Key(@NotNull String name) {
		this(Name.of(name));
	}

	public Key(@NotNull Class<? extends Annotation> annotationType) {
		this(Name.of(annotationType));
	}

	public Key(@NotNull Annotation annotation) {
		this(Name.of(annotation));
	}

	/**
	 * A default subclass to be used by {@link #of Key.of*} and {@link #ofType Key.ofType*} constructors
	 */
	private static final class KeyImpl<T> extends Key<T> {
		private KeyImpl(@NotNull AnnotatedType annotatedType) {
			super(annotatedType);
		}

		private KeyImpl(@NotNull AnnotatedType annotatedType, @Nullable Name name) {
			super(name != null ? Types.annotate(annotatedType, name.getAnnotation()) : annotatedType, name);
		}

		private KeyImpl(@NotNull Type type, @Nullable Name name) {
			super(name != null ? Types.annotate(type, name.getAnnotation()) : Types.annotate(type), name);
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

	@NotNull
	public static <T> Key<T> ofType(@NotNull AnnotatedType type) {
		return new KeyImpl<>(type, null);
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull AnnotatedType type, @Nullable Name name) {
		return new KeyImpl<>(type, name);
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull AnnotatedType type, @NotNull String name) {
		return new KeyImpl<>(type, Name.of(name));
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull AnnotatedType type, @NotNull Class<? extends Annotation> annotationType) {
		return new KeyImpl<>(type, Name.of(annotationType));
	}

	@NotNull
	public static <T> Key<T> ofType(@NotNull AnnotatedType type, @NotNull Annotation annotation) {
		return new KeyImpl<>(type, Name.of(annotation));
	}

	/**
	 * Returns a new key with same type but the name replaced with a given one
	 */
	public Key<T> named(Name name) {
		return new KeyImpl<>(annotatedType, name);
	}

	/**
	 * @see #named(Name)
	 */
	public Key<T> named(String name) {
		return new KeyImpl<>(annotatedType, Name.of(name));
	}

	/**
	 * @see #named(Name)
	 */
	public Key<T> named(@NotNull Class<? extends Annotation> annotationType) {
		return new KeyImpl<>(annotatedType, Name.of(annotationType));
	}

	/**
	 * @see #named(Name)
	 */
	public Key<T> named(@NotNull Annotation annotation) {
		return new KeyImpl<>(annotatedType, Name.of(annotation));
	}

	private AnnotatedType retrieveType() {
		// this cannot possibly fail so not even a check here
		return ((AnnotatedParameterizedType) getClass().getAnnotatedSuperclass()).getAnnotatedActualTypeArguments()[0];
	}

	@NotNull
	public AnnotatedType getAnnotatedType() {
		return annotatedType;
	}

	@NotNull
	public Type getType() {
		return annotatedType.getType();
	}

	/**
	 * A shortcut for <code>{@link Types#getRawType}(key.getType())</code>.
	 * Also casts the result to a properly parameterized class.
	 */
	@SuppressWarnings("unchecked")
	@NotNull
	public Class<T> getRawType() {
		return (Class<T>) Types.getRawType(annotatedType.getType());
	}

	/**
	 * Returns a type argument of the underlying type wrapped as a key.
	 * <p>
	 * If a {@link io.datakernel.di.annotation.NameAnnotation name annotation} is present on that
	 * type argument, then it is used as a name for that key.
	 * <p>
	 * <b>Note:</b>
	 * In Java 8 type argument name annotations work only if key tag subclass was created not in another inner class
	 * (its enclosing class itself has no enclosing class) because of a reflection bug.
	 * It was fixed only in Java 9.
	 *
	 * @throws IllegalStateException when underlying type is not a parameterized one.
	 */
	public <U> Key<U> getTypeArgument(int index) {
		if (annotatedType instanceof AnnotatedParameterizedType) {
			return new KeyImpl<>(((AnnotatedParameterizedType) annotatedType).getAnnotatedActualTypeArguments()[index]);
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
		return (name != null ? name.getDisplayString() + " " : "") + ReflectionUtils.getShortName(annotatedType.getType().getTypeName());
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
		return annotatedType.getType().equals(that.annotatedType.getType()) && Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return 31 * annotatedType.getType().hashCode() + (name != null ? name.hashCode() : 0);
	}

	@Override
	public String toString() {
		return (name != null ? name.toString() + " " : "") + annotatedType.getType().getTypeName();
	}
}
