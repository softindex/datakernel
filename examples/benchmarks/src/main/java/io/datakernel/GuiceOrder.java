package io.datakernel;

import com.google.inject.BindingAnnotation;
import com.google.inject.ScopeAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @Author is Alex Syrotenko (@pantokrator)
 * Created on 24.07.19.
 */
@ScopeAnnotation
@BindingAnnotation
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RUNTIME)
public @interface GuiceOrder {
}
