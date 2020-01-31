package io.global.ot.service;

import io.datakernel.di.annotation.ScopeAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@ScopeAnnotation
@Retention(RUNTIME)
@Target(METHOD)
public @interface ContainerScope {
}

