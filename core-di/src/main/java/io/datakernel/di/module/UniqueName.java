package io.datakernel.di.module;

import io.datakernel.di.annotation.NameAnnotation;

import java.lang.annotation.Target;

@NameAnnotation
@Target({}) // no target since this is a dummy annotation type
public @interface UniqueName {
}
