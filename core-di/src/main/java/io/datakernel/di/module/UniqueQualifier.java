package io.datakernel.di.module;

import io.datakernel.di.annotation.QualifierAnnotation;

import java.lang.annotation.Target;

@QualifierAnnotation
@Target({}) // no target since this is a dummy annotation type
public @interface UniqueQualifier {
}
