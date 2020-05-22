package io.datakernel.remotefs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is for default methods that semantically describe
 * themselves in terms of other interface methods, but must have an
 * actual efficient implementation that follows these semantics.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface MustImplement {
}
