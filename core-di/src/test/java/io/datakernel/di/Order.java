package io.datakernel.di;

import io.datakernel.di.annotation.ScopeAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @Author is Alex Syrotenko (@pantokrator)
 * Created on 12.07.19.
 */
@ScopeAnnotation
@Target({ElementType.METHOD})
@Retention(RUNTIME)
public @interface Order {
}
