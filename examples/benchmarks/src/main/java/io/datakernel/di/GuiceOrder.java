package io.datakernel.di;

import com.google.inject.BindingAnnotation;
import com.google.inject.ScopeAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@ScopeAnnotation
@BindingAnnotation
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RUNTIME)
public @interface GuiceOrder {
}
