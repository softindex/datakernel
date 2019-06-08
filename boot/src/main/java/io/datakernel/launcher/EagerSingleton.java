package io.datakernel.launcher;

import io.datakernel.di.KeySetAnnotation;
import io.datakernel.di.NameAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@NameAnnotation
@KeySetAnnotation
public @interface EagerSingleton {
}
