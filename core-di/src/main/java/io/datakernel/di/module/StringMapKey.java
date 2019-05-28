package io.datakernel.di.module;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@MapKey(unwrapValue = true)
@Target(METHOD)
@Retention(RUNTIME)
public @interface StringMapKey {
  String value();
}
