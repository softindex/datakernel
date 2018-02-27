package io.datakernel.worker;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation marker for binding one object which is 'primary' of its kind.
 * For example, primary evenloop among worker eventloops and so on.
 */
@BindingAnnotation
@Target({PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface Primary {
}
