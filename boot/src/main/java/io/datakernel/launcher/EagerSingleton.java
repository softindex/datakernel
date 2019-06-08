package io.datakernel.launcher;

import io.datakernel.di.KeyGroupAnnotation;
import io.datakernel.di.NameAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@NameAnnotation
@KeyGroupAnnotation
public @interface EagerSingleton {
}
