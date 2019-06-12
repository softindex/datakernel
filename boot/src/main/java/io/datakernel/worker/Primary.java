package io.datakernel.worker;

import io.datakernel.di.annotation.NameAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation marker for binding one object which is 'primary' of its kind.
 * For example, primary evenloop among worker eventloops and so on.
 */
@NameAnnotation
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface Primary {
}
