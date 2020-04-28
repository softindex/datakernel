package io.datakernel.di.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a built-in stateful annotation.
 * <p>
 * It is used for quick prototyping or when you need too many different keys with the same type.
 * <p>
 * If you have only few distinct <i>groups</i> of objects with the same type, consider making your own {@link QualifierAnnotation qualifier annotations}.
 */
@QualifierAnnotation
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface Named {
	String value();
}
