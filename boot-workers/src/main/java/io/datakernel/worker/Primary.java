package io.datakernel.worker;

import io.datakernel.di.annotation.QualifierAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation marker for binding one object which is 'primary' of its kind.
 * For example, primary eventloop among worker eventloops and so on.
 */
@QualifierAnnotation
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface Primary {
}
